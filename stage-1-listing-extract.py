import asyncio
import curl_cffi
from bs4 import BeautifulSoup
from url_generator import URLGenerator
import os
import json
import hashlib
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import time
from datetime import datetime
from typing import Dict, List, Tuple
from pymongo.mongo_client import MongoClient
import logging

from pymongo.server_api import ServerApi
from price_tracker import PriceTracker

load_dotenv()

# California rotating proxy
california_rotating_proxy = {
    'https': 'http://35d146gk4kq9otn-country-us-state-california:z51vlpkz84emlb9@rp.scrapegw.com:6060'
}

class AsyncStage1DataExtract:
    def __init__(self, max_concurrent=5):
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.homepagedata_collection = None
        self.temp_collection = None
        self.archivedlistings_collection = None
        # Initialize queue system for failed requests
        self.retry_queue = asyncio.Queue()
        self.failure_queue = []  # For persistent retry
        self.failed_urls = []
        self.delay_between_requests = 5.0
        self.max_retries_per_url = 3
        self.retry_attempts = {}  # Track retry attempts per URL
        # Browser impersonation rotation for first attempts
        self.impersonations = ["chrome", "safari", "firefox", "chrome_android", "safari_ios"]
        self.impersonation_index = 0

    def Generate_URLs(self):
        """
        Generate URLs for the scraper to scrape.
        """
        url_generator = URLGenerator()
        urls_to_scrape = url_generator.generate_urls()
        request_settings = url_generator.get_request_settings()
        return urls_to_scrape, request_settings
    
    def fallback_listing_id(self, listing_data):
        """
        Fallback listing ID generation.
        Returns None if no suitable fields are found - indicates potential new format.
        """
        key_fields = ["name", "address", "@id", "identifier"]
        id_parts = []
        for field in key_fields:
            if field in listing_data:
                id_parts.append(str(listing_data[field]))
        
        if id_parts:
            fallback_id = hashlib.md5("|".join(id_parts).encode()).hexdigest()
            logging.warning(f"🔄 Using fallback ID generation: {fallback_id}")
            logging.warning(f"   📋 Based on fields: {[f for f in key_fields if f in listing_data]}")
            return fallback_id
        
        # No fallback fields available - potential new format
        logging.warning(f"🚨 FALLBACK FAILED - No suitable fields found for ID generation")
        logging.warning(f"   📋 Searched for: {key_fields}")
        logging.warning(f"   📋 Available keys: {list(listing_data.keys())}")
        return None
    
    async def archive_removed_listings(self, removed_listing_ids):
        """
        Move removed listings to archivedlistings collection with timestamp and reason.
        """
        if not removed_listing_ids:
            return 0
            
        archived_count = 0
        for listing_id in removed_listing_ids:
            try:
                # Find the listing in homepagedata
                listing = await self.homepagedata_collection.find_one({"listing_id": listing_id})
                if listing:
                    # Add archive metadata
                    listing["archived_at"] = datetime.now()
                    listing["archive_reason"] = "listings no longer available"
                    listing["original_scraped_at"] = listing.get("scraped_at")
                    
                    # Insert to archive collection
                    await self.archivedlistings_collection.insert_one(listing)
                    
                    # Remove from homepagedata
                    await self.homepagedata_collection.delete_one({"listing_id": listing_id})
                    
                    # NEW: Consolidate price history to permanent storage
                    await self._consolidate_price_history(listing_id)
                    
                    archived_count += 1
                    logging.info(f"📦 Archived listing: {listing_id}")
                    
            except Exception as e:
                logging.error(f"❌ Failed to archive listing {listing_id}: {e}")
                
        return archived_count

    async def archive_missing_listings(self, missing_listing_ids):
        """
        Archive listings that are missing from current scrape (immediate removal detection).
        """
        if not missing_listing_ids:
            return 0
            
        archived_count = 0
        for listing_id in missing_listing_ids:
            try:
                # Find the listing in homepagedata
                listing = await self.homepagedata_collection.find_one({"listing_id": listing_id})
                if listing:
                    # Add archive metadata
                    listing["archived_at"] = datetime.now()
                    listing["archive_reason"] = "missing from current scrape"
                    listing["original_scraped_at"] = listing.get("scraped_at")
                    
                    # Insert to archive collection
                    await self.archivedlistings_collection.insert_one(listing)
                    
                    # Remove from homepagedata
                    await self.homepagedata_collection.delete_one({"listing_id": listing_id})
                    
                    # NEW: Consolidate price history to permanent storage
                    await self._consolidate_price_history(listing_id)
                    
                    archived_count += 1
                    logging.info(f"📦 Archived missing listing: {listing_id}")
                    
            except Exception as e:
                logging.error(f"❌ Failed to archive missing listing {listing_id}: {e}")
                
        return archived_count

    async def remove_update_add_listings(self, current_scraped_data, existing_db_ids):
        """
        CRITICAL: Remove, Update, Add Listings with proper data flagging.
        Returns processed data with status flags: 'new', 'updated', 'removed', 'active'
        """
        # Extract listing_id from documents (not from raw JSON-LD data)
        current_scraped_ids = {item.get("listing_id") for item in current_scraped_data if item.get("listing_id")}
        
        current_set = set(current_scraped_ids)
        existing_set = set(existing_db_ids)
        
        changes = {
            "new_listings": current_set - existing_set,
            "removed_listings": existing_set - current_set,
            "active_listings": current_set & existing_set
        }
        
        # Real-time comparison logging
        logging.info(f"🔍 REAL-TIME COMPARISON:")
        logging.info(f"   📊 Current scraped: {len(current_set)} listings")
        logging.info(f"   💾 Previously in DB: {len(existing_set)} listings")
        logging.info(f"   🆕 NEW listings: {len(changes['new_listings'])}")
        if changes['new_listings']:
            for new_url in list(changes['new_listings'])[:3]:  # Show first 3
                logging.info(f"      ➕ {new_url}")
            if len(changes['new_listings']) > 3:
                logging.info(f"      ... and {len(changes['new_listings']) - 3} more")
        
        logging.info(f"   🗑️ REMOVED listings: {len(changes['removed_listings'])}")
        if changes['removed_listings']:
            for removed_url in list(changes['removed_listings'])[:3]:  # Show first 3
                logging.info(f"      ❌ {removed_url}")
            if len(changes['removed_listings']) > 3:
                logging.info(f"      ... and {len(changes['removed_listings']) - 3} more")
        
        logging.info(f"   ✅ ACTIVE listings: {len(changes['active_listings'])}")
        
        # Archive removed listings
        archived_count = await self.archive_removed_listings(changes["removed_listings"])
        logging.info(f"📦 Archived {archived_count} removed listings")
        
        # Process current scraped data with status flags
        processed_data = []
        for item in current_scraped_data:
            listing_id = item.get("listing_id")
            if not listing_id:
                continue
                
            # Add status flag to the data
            if listing_id in changes["new_listings"]:
                item["listing_status"] = "new"
                item["status_updated_at"] = datetime.now()
            elif listing_id in changes["active_listings"]:
                # Check if data actually changed (simplified - compare key fields)
                item["listing_status"] = "active"  # Could be "updated" after data comparison
                item["status_updated_at"] = datetime.now()
            
            # Keep the source_url as set during document creation
            # listing_id format is URL_Name, so extract URL for source_url if not already set
            if "source_url" not in item and "_" in listing_id:
                potential_url = listing_id.split("_")[0]
                if potential_url.startswith("http"):
                    item["source_url"] = potential_url
            
            item["listing_id"] = listing_id
            
            processed_data.append(item)
            
        return processed_data, changes

    async def detect_and_archive_stale_listings(self, max_age_days=3):
        """
        Detect and archive listings that haven't been scraped in a while.
        This is a separate process from the main scraping workflow.
        """
        cutoff_time = datetime.now().timestamp() - (max_age_days * 24 * 60 * 60)
        
        try:
            # Find listings not scraped recently
            stale_docs = await self.homepagedata_collection.find({
                "scraped_at": {"$lt": cutoff_time}
            }).to_list(length=None)
            
            if stale_docs:
                logging.info(f"🗑️ Found {len(stale_docs)} stale listings (>{max_age_days} days old)")
                
                # Archive stale listings
                for doc in stale_docs:
                    doc["archived_at"] = datetime.now()
                    doc["archive_reason"] = f"not scraped for {max_age_days} days"
                    doc["original_scraped_at"] = doc.get("scraped_at")
                    
                    await self.archivedlistings_collection.insert_one(doc)
                    await self.homepagedata_collection.delete_one({"_id": doc["_id"]})
                
                logging.info(f"📦 Archived {len(stale_docs)} stale listings")
                return len(stale_docs)
            else:
                logging.info(f"✅ No stale listings found (cutoff: {max_age_days} days)")
                return 0
                
        except Exception as e:
            logging.error(f"❌ Error detecting stale listings: {e}")
            return 0

    async def process_listings_batch(self, extracted_data):
        """
        Process a batch of listings with proper individual database operations.
        This replaces the flawed remove_update_add_listings workflow.
        """
        if not extracted_data:
            return set()
            
        processed_count = {"new": 0, "updated": 0, "unchanged": 0}
        scraped_listing_ids = set()
        new_listing_ids = []
        
        for document in extracted_data:
            listing_id = document.get("listing_id")
            if not listing_id:
                continue
                
            scraped_listing_ids.add(listing_id)
            
            try:
                # Check if listing exists in database
                existing_doc = await self.homepagedata_collection.find_one(
                    {"listing_id": listing_id},
                    {"listing_id": 1, "scraped_at": 1, "property_data": 1, "data_source": 1}
                )
                
                if existing_doc:
                    # Compare data to see if update is needed
                    needs_update = self.has_listing_changed(existing_doc, document)
                    
                    # Log data source information for monitoring
                    existing_source = existing_doc.get("data_source", "unknown")
                    new_source = document.get("data_source", "unknown")
                    
                    if needs_update:
                        document["listing_status"] = "updated"
                        document["last_updated"] = datetime.now()
                        document["previous_scraped_at"] = existing_doc.get("scraped_at")
                        document["previous_data_source"] = existing_source
                        
                        result = await self.homepagedata_collection.replace_one(
                            {"listing_id": listing_id},
                            document
                        )
                        processed_count["updated"] += 1
                        logging.info(f"🔄 Updated listing: {listing_id} (source: {existing_source}→{new_source}, matched: {result.matched_count}, modified: {result.modified_count})")
                    else:
                        # Just update the scraped_at timestamp
                        result = await self.homepagedata_collection.update_one(
                            {"listing_id": listing_id},
                            {"$set": {"scraped_at": datetime.now(), "listing_status": "active"}}
                        )
                        processed_count["unchanged"] += 1
                        logging.info(f"✅ Listing timestamp updated: {listing_id} (source: {new_source}, matched: {result.matched_count}, modified: {result.modified_count})")
                else:
                    # New listing
                    document["listing_status"] = "new"
                    document["first_scraped_at"] = datetime.now()
                    
                    result = await self.homepagedata_collection.insert_one(document)
                    processed_count["new"] += 1
                    new_listing_ids.append(listing_id)
                    logging.info(f"🆕 New listing inserted: {listing_id} (ObjectId: {result.inserted_id})")
                    
            except Exception as e:
                logging.error(f"❌ Error processing listing {listing_id}: {e}")
                
        # Log summary with new listings details
        total_db_operations = sum(processed_count.values())
        logging.info(f"📊 Batch processed: {processed_count['new']} new, {processed_count['updated']} updated, {processed_count['unchanged']} unchanged")
        logging.info(f"💾 Total database operations: {total_db_operations} successful upserts")
        
        if new_listing_ids:
            logging.info(f"🆕 NEW LISTINGS FOUND:")
            for new_id in new_listing_ids[:5]:  # Show first 5
                logging.info(f"   ✨ {new_id}")
            if len(new_listing_ids) > 5:
                logging.info(f"   ... and {len(new_listing_ids) - 5} more new listings")
        
        return scraped_listing_ids
        
    def has_listing_changed(self, existing_doc, new_doc):
        """
        Compare existing and new listing data to determine if update is needed.
        Handles both JSON-LD and HTML fallback data structures.
        Returns True if data has changed significantly, False otherwise.
        """
        existing_data = existing_doc.get("property_data", {})
        new_data = new_doc.get("property_data", {})
        
        # Get data source information
        existing_source = existing_doc.get("data_source", "unknown")
        new_source = new_doc.get("data_source", "unknown")
        
        # If data source changed, it's worth updating to track the change
        if existing_source != new_source:
            logging.debug(f"🔍 Data source changed: {existing_source} → {new_source}")
            return True
        
        # Extract standardized values for comparison
        existing_standardized = self._extract_comparable_values(existing_data, existing_source)
        new_standardized = self._extract_comparable_values(new_data, new_source)
        
        # Compare standardized values
        changes_detected = []
        for field, (existing_value, new_value) in zip(
            existing_standardized.keys(), 
            zip(existing_standardized.values(), new_standardized.values())
        ):
            if existing_value != new_value:
                changes_detected.append(f"{field}: {existing_value} → {new_value}")
        
        if changes_detected:
            logging.debug(f"🔍 Changes detected: {'; '.join(changes_detected)}")
            return True
                
        return False
    
    def _extract_comparable_values(self, data, data_source):
        """
        Extract comparable values from either JSON-LD or HTML data structures.
        Returns standardized dict for comparison.
        """
        if data_source == "json_ld":
            return {
                "name": data.get("name"),
                "url": data.get("url"),
                "price": self.get_nested_value(data, "offers.price"),
                "availability": self.get_nested_value(data, "offers.availability"),
                "address": self._standardize_address(data.get("address")),
                "telephone": data.get("telephone"),
                "condition": data.get("itemCondition")
            }
        elif data_source == "html_fallback":
            return {
                "name": data.get("name"),
                "url": data.get("url"), 
                "price": self._standardize_price(data.get("price")),
                "availability": None,  # Not available in HTML fallback
                "address": self._standardize_address(data.get("address")),
                "telephone": None,  # Not available in HTML fallback
                "condition": None   # Not available in HTML fallback
            }
        else:
            # Fallback for unknown data source
            return {
                "name": data.get("name"),
                "url": data.get("url"),
                "price": str(data.get("price", "")),
                "availability": None,
                "address": str(data.get("address", "")),
                "telephone": data.get("telephone"),
                "condition": data.get("itemCondition")
            }
    
    def _standardize_address(self, address_data):
        """Standardize address to string format for comparison."""
        if isinstance(address_data, dict):
            # JSON-LD format: extract readable address
            parts = []
            if address_data.get("streetAddress"):
                parts.append(address_data["streetAddress"])
            if address_data.get("addressLocality"):
                parts.append(address_data["addressLocality"])
            if address_data.get("addressRegion"):
                parts.append(address_data["addressRegion"])
            if address_data.get("postalCode"):
                parts.append(address_data["postalCode"])
            return ", ".join(parts)
        elif isinstance(address_data, str):
            # HTML format: already a string
            return address_data
        else:
            return str(address_data) if address_data else None
    
    def _standardize_price(self, price_data):
        """Standardize price to comparable format."""
        if not price_data:
            return None
        
        # Remove common formatting
        price_str = str(price_data).replace("$", "").replace(",", "").strip()
        
        # Try to extract numeric value
        try:
            # Handle cases like "$850,000" or "850000"
            return price_str
        except:
            return price_str
        
    def get_nested_value(self, data, key_path):
        """
        Get nested value from dict using dot notation (e.g., 'offers.price').
        Returns None if path doesn't exist.
        """
        keys = key_path.split('.')
        value = data
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
                
        return value

    async def connect_to_mongodb(self):
        """Async MongoDB connection"""
        try:
            uri = os.getenv("MONGO_DB_URI")
            client = AsyncIOMotorClient(uri)
            db = client['newhomesource']
            self.homepagedata_collection = db['homepagedata']
            self.temp_collection = db['temphtml']
            self.archivedlistings_collection = db['archivedlistings']

            #pin the database
            await client.admin.command('ping')
            logging.info("✅ Successfully connected to MongoDB!")

            return client
        except Exception as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            raise Exception(f"Failed to connect to MongoDB: {e}")
    
    async def fetch_without_proxy(self, url: str, impersonation: str = "chrome", max_retries: int = 3) -> Tuple[int, str]:
        """Async HTTP request using curl_cffi with browser rotation on failure"""
        async with self.semaphore:
            last_exception = None
            current_impersonation = impersonation
            impersonation_idx = self.impersonations.index(impersonation) if impersonation in self.impersonations else 0
            
            for attempt in range(max_retries + 1):
                try:
                    # Add delay between requests for rate limiting
                    if attempt > 0:
                        backoff_delay = min(2 ** attempt, 30) # Cap at 30 seconds
                        logging.info(f"🔄 Retry {attempt} for {url}, waiting {backoff_delay}s...")
                        await asyncio.sleep(backoff_delay)
                    elif hasattr(self, 'delay_between_requests'):
                        await asyncio.sleep(self.delay_between_requests)
                    
                    # Use curl_cffi session with current impersonation
                    def make_request():
                        with curl_cffi.Session(impersonate=current_impersonation) as session:
                            return session.get(url, proxies=california_rotating_proxy, timeout=15, verify=False)
                    
                    response = await asyncio.to_thread(make_request)
                    
                    logging.info(f"🌐 Fetched {url} → Status: {response.status_code} (using {current_impersonation})")
                    
                    # Success for any 2xx status code
                    if 200 <= response.status_code < 300:
                        if attempt > 0:
                            logging.info(f"✅ Request succeeded on attempt {attempt + 1} for {url} with {current_impersonation}")
                        return response.status_code, response.text
                    
                    # Handle specific error codes - rotate browser on failure
                    elif response.status_code in [403, 429, 500, 502, 503, 504]:    
                        logging.warning(f"⚠️ HTTP {response.status_code} for {url} with {current_impersonation} (attempt {attempt + 1})")
                        if attempt < max_retries:
                            # Rotate to next browser impersonation
                            impersonation_idx = (impersonation_idx + 1) % len(self.impersonations)
                            current_impersonation = self.impersonations[impersonation_idx]
                            logging.info(f"🔄 Rotating to {current_impersonation} for next attempt")
                        else:
                            return response.status_code, ""
                        continue
                    else:
                        # Don't retry for other 4xx errors (404, 400, etc.)
                        logging.error(f"❌ HTTP {response.status_code} error for {url} - not retrying")
                        return response.status_code, ""
                    
                except Exception as e:
                    last_exception = e
                    logging.warning(f"⚠️ Request exception for {url} with {current_impersonation} (attempt {attempt + 1}): {e}")
                    if attempt < max_retries:
                        # Rotate browser on exception too
                        impersonation_idx = (impersonation_idx + 1) % len(self.impersonations)
                        current_impersonation = self.impersonations[impersonation_idx]
                        logging.info(f"🔄 Exception triggered rotation to {current_impersonation}")
                    elif attempt == max_retries:
                        break
            
            # All retries failed
            logging.error(f"❌ All {max_retries + 1} attempts failed for {url} (tried {max_retries + 1} different browsers)")
            if last_exception:
                logging.error(f"❌ Last exception: {last_exception}")
            return 0, ""
    
    async def process_single_url(self, temp_collection, _id:str, url:str, impersonation:str="chrome", location_info:dict=None):
        """
        Process single URL and determine listing status (new/update/removal).
        Integrates with remove_update_add_listings workflow.
        """
        try:
            # Fetch the URL
            status_code, html = await self.fetch_without_proxy(url, impersonation)
            
            if status_code != 200 or not html:
                logging.warning(f"⚠️ Failed to fetch {url} (status: {status_code})")
                await self.retry_queue.put((url, impersonation, location_info))
                return False
                
            # Store in temp collection
            await temp_collection.insert_one({"_id": _id, "url": url, "html": html})
            
            # Parse and process with location info
            result = await self.parse_html(url, html, location_info)
            
            # Clean up temp
            await temp_collection.delete_one({"_id": _id})
            
            # Return scraped IDs if successful, empty set if failed
            if isinstance(result, set):
                return result
            else:
                return set() if result else set()
            
        except Exception as e:
            logging.error(f"❌ Error processing {url}: {e}")
            await self.retry_queue.put((url, impersonation, location_info))
            return False
        
    async def process_retry_queue(self):
        """Process the retry queue with different browser impersonations and send failures to back of queue."""
        impersonations = ["chrome", "safari", "firefox", "chrome_android", "safari_ios"]
        processed_urls = set()  # Track processed URLs to avoid infinite loops
        
        while not self.retry_queue.empty():
            try:
                url, last_impersonation, location_info = await asyncio.wait_for(self.retry_queue.get(), timeout=1.0)
                
                # Avoid infinite loops
                if url in processed_urls:
                    continue
                    
                # Check retry limit for queue processing
                if url not in self.retry_attempts:
                    self.retry_attempts[url] = 0
                    
                if self.retry_attempts[url] >= self.max_retries_per_url:
                    logging.warning(f"⚠️ Queue retries exceeded for {url}, moving to failure queue")
                    self.failure_queue.append((url, location_info))
                    processed_urls.add(url)
                    continue
                
                # Try next impersonation
                current_idx = impersonations.index(last_impersonation) if last_impersonation in impersonations else 0
                next_impersonation = impersonations[(current_idx + 1) % len(impersonations)]
                
                self.retry_attempts[url] += 1
                logging.info(f"🔄 Queue retry {self.retry_attempts[url]}/{self.max_retries_per_url} for {url} with {next_impersonation}")
                
                # Generate unique ID for temp collection
                temp_id = f"{url}_{int(datetime.now().timestamp())}_{self.retry_attempts[url]}"
                success = await self.process_single_url(self.temp_collection, temp_id, url, next_impersonation, location_info)
                
                if success:
                    logging.info(f"✅ Queue retry successful for {url}")
                    processed_urls.add(url)
                    # Remove from retry tracking
                    if url in self.retry_attempts:
                        del self.retry_attempts[url]
                else:
                    # Send back to end of queue (but not infinitely)
                    if self.retry_attempts[url] < self.max_retries_per_url:
                        await self.retry_queue.put((url, next_impersonation, location_info))
                    await asyncio.sleep(2)
                    
            except asyncio.TimeoutError:
                break  # No more items in queue
            except Exception as e:
                logging.error(f"❌ Error processing retry queue: {e}")

    async def persistent_failure_retry(self, max_duration_minutes=30):
        """30-minute persistent retry with exponential backoff and impersonation rotation for failure queue."""
        if not self.failure_queue:
            return
            
        start_time = time.time()
        max_duration = max_duration_minutes * 60
        impersonations = ["chrome", "safari", "firefox", "chrome_android", "safari_ios"]
        remaining_failures = self.failure_queue.copy()
        attempt = 0
        
        logging.info(f"🔄 Starting 30-minute persistent retry for {len(remaining_failures)} failed URLs")
        
        while remaining_failures and (time.time() - start_time) < max_duration:
            attempt += 1
            # Exponential backoff: 30s, 60s, 120s, 240s, 480s, then cap at 10 minutes
            backoff_delay = min(30 * (2 ** (attempt - 1)), 600)
            current_impersonation = impersonations[(attempt - 1) % len(impersonations)]
            
            logging.info(f"🔄 Persistent attempt {attempt} using {current_impersonation} (backoff: {backoff_delay}s)")
            
            if attempt > 1:
                await asyncio.sleep(backoff_delay)
            
            if (time.time() - start_time) >= max_duration:
                logging.warning(f"⏰ 30-minute timeout reached")
                break
            
            # Try all remaining failures
            new_remaining = []
            for url, location_info in remaining_failures:
                temp_id = f"{url}_{int(datetime.now().timestamp())}_persistent_{attempt}"
                success = await self.process_single_url(self.temp_collection, temp_id, url, current_impersonation, location_info)
                
                if not success:
                    new_remaining.append((url, location_info))
                else:
                    logging.info(f"✅ Persistent retry success: {url}")
            
            success_count = len(remaining_failures) - len(new_remaining)
            logging.info(f"📊 Persistent attempt {attempt}: {success_count}/{len(remaining_failures)} succeeded")
            remaining_failures = new_remaining
            
            if not remaining_failures:
                logging.info(f"🎉 All failure queue URLs recovered!")
                break
        
        # Final summary
        elapsed = time.time() - start_time
        total_failed = len(self.failure_queue)
        final_remaining = len(remaining_failures)
        total_recovered = total_failed - final_remaining
        
        logging.info(f"📊 Persistent retry summary ({elapsed:.1f}s):")
        logging.info(f"   ✅ Recovered: {total_recovered}/{total_failed}")
        logging.info(f"   ❌ Still failed: {final_remaining}/{total_failed}")
        
        if remaining_failures:
            self.failed_urls.extend([url for url, _ in remaining_failures])
            logging.warning(f"⚠️ {len(remaining_failures)} URLs permanently failed")
        
        # Clear failure queue
        self.failure_queue.clear()
                
    async def retry_with_different_impersonation(self, temp_collection, _id:str, url:str) -> bool:
        """Legacy method - now uses process_retry_queue system"""
        await self.retry_queue.put((url, "chrome", {}))
        await self.process_retry_queue()
        return url not in self.failed_urls
    
    def setup_logging(self):
        """Setup logging for the scraper session"""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(script_dir, "logging", "nhs")
        os.makedirs(log_dir, exist_ok=True)
        
        log_filename = f"scraper_log_{time.strftime('%Y%m%d_%H%M%S')}.log"
        log_filepath = os.path.join(log_dir, log_filename)
        
        # Create file handler
        file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        logger = logging.getLogger(f"{__name__}_{id(self)}")
        logger.setLevel(logging.DEBUG)  # Enable debug logging
        logger.addHandler(file_handler)
        
        return logger, log_filename

    async def scrape_nhs_base_urls(self, urls_with_info:list, request_settings:dict):
        """
        Main async scraping logic with proper error handling and queue processing.
        """
        logger, logfile = self.setup_logging()
        
        logger.info("Starting NewHomeSource async scraping session")
        logger.info(f"🌐 Browser impersonation: {request_settings['impersonation']}")
        # Handle both URL formats: just URLs or (url, location_info) tuples
        if urls_with_info and isinstance(urls_with_info[0], tuple):
            urls = [url_info[0] for url_info in urls_with_info]
            self.url_location_map = {url_info[0]: url_info[1] for url_info in urls_with_info}
        else:
            urls = urls_with_info
            self.url_location_map = {}
        
        logger.info(f"📊 Total URLs to process: {len(urls)}")
        
        # Connect to MongoDB
        try:
            client = await self.connect_to_mongodb()
            logger.info("✅ MongoDB connection established")
        except Exception as e:
            logger.error(f"❌ MongoDB connection failed: {e}")
            return 1, logfile, False
        
        success_count = 0
        error_count = 0
        all_scraped_listings = set()
        
        # Get existing listings for removal detection
        existing_listings = set(await self.homepagedata_collection.distinct("listing_id"))
        logger.info(f"📊 Found {len(existing_listings)} existing listings in database")
        
        # Process all URLs with rotating impersonation
        for idx, url in enumerate(urls, 1):
            try:
                # Rotate impersonation for first attempts
                current_impersonation = self.impersonations[self.impersonation_index % len(self.impersonations)]
                self.impersonation_index += 1
                
                logger.info(f"🔍 Processing {idx}/{len(urls)}: {url} (using {current_impersonation})")
                temp_id = f"{url}_{int(datetime.now().timestamp())}_{idx}"
                
                # Pass location info if available
                location_info = self.url_location_map.get(url, {})
                result = await self.process_single_url(self.temp_collection, temp_id, url, current_impersonation, location_info)
                
                if isinstance(result, set):
                    all_scraped_listings.update(result)
                    success_count += 1
                    logger.info(f"✅ Successfully processed {url}")
                else:
                    error_count += 1
                    
                # Rate limiting delay
                if idx < len(urls):
                    await asyncio.sleep(self.delay_between_requests)
                    
            except Exception as e:
                error_msg = f"❌ Failed to process {url}: {e}"
                logger.error(error_msg)
                error_count += 1
        
        # Process retry queue for failed requests
        logger.info("🔄 Processing retry queue...")
        await self.process_retry_queue()
        
        # Process failure queue with 30-minute persistent retry
        logger.info("🔄 Processing failure queue with persistent retry...")
        await self.persistent_failure_retry(max_duration_minutes=30)
        
        # Immediate removal detection - archive missing listings
        missing_listings = existing_listings - all_scraped_listings
        if missing_listings:
            # Safety check: prevent mass archival due to format changes
            missing_percentage = len(missing_listings) / len(existing_listings) if existing_listings else 0
            
            if missing_percentage > 0.5:  # >50% missing
                logger.error(f"🚨 SAFETY CHECK TRIGGERED: {len(missing_listings)}/{len(existing_listings)} listings appear missing ({missing_percentage:.1%})")
                logger.error("🚨 This may indicate a data format change or scraping issue")
                logger.error("🚨 Skipping mass archival - manual investigation required")
                logger.error(f"🚨 Scraped listings found: {len(all_scraped_listings)}")
                archived_missing = 0
            else:
                logger.info(f"🗑️ Found {len(missing_listings)} missing listings - archiving immediately")
                archived_missing = await self.archive_missing_listings(missing_listings)
                logger.info(f"📦 Archived {archived_missing} missing listings")
        else:
            logger.info("✅ No missing listings detected")
            archived_missing = 0
        
        # Final summary
        logger.info(f"📊 Session complete: {success_count} success, {error_count} errors")
        logger.info(f"❌ Failed URLs: {len(self.failed_urls)}")
        logger.info(f"📦 Missing listings archived: {archived_missing}")
        logger.info(f"🔍 Total scraped listings: {len(all_scraped_listings)}")
        
        # Clean up temp collection
        try:
            deleted_temp = await self.temp_collection.delete_many({})
            logger.info(f"🧹 Cleaned up {deleted_temp.deleted_count} temp documents")
        except Exception as e:
            logger.warning(f"⚠️ Failed to clean temp collection: {e}")
        
        if client:
            client.close()
            logger.info("🔌 MongoDB connection closed")
        
        overall_success = success_count > 0 and error_count < len(urls) * 0.5  # 50% success threshold
        return (0 if overall_success else 1), logfile, overall_success    
    
    def get_nhs_card_listing_id(self, data_dict):
        """
        Generate a listing_id for a nhs card listing. URL + Name
        """
        # Get the url from the data_dict
        url = data_dict.get("url")
        # Get the name from the data_dict
        name = data_dict.get("name")
        
        # Validate required fields
        if not url or not name:
            logging.warning(f"🚨 Missing required fields for HTML listing ID:")
            logging.warning(f"   📋 URL: {url}")
            logging.warning(f"   📋 Name: {name}")
            return None
        
        # Add underscores in spaces between words in name
        name = name.replace(" ", "_")
        
        #Primary key is the url + name
        primary_key = f"{url}_{name}"

        # Return the primary key
        return primary_key

    def get_script_listing_id(self, listing_data):
        """
        Generate unique identifier for listing based on URL and key attributes. Listing URL + Name. 
        {"@context":"https://schema.org","@type":["Product","SingleFamilyResidence"],"name":"Fresco at Del Sol subdivision by Shea Homes",
        "image":{"@type":"ImageObject","name":"Fresco at Del Sol community","description":"Image of the Fresco at Del Sol subdivision by Shea Homes","audience":"Homebuyers","contentLocation":"Fresco at Del Sol","copyrightHolder":"Shea Homes","creator":"Shea Homes","creditText":"Photo Credit: Shea Homes","isFamilyFriendly":true,"contentUrl":"https://nhs-dynamic-secure.akamaized.net/Images/Homes/Shea/83251101-240906.webp"},
        "url":"https://www.newhomesource.com/community/ca/ventura/fresco-at-del-sol-by-shea-homes/176723","address":{"@type":"PostalAddress","addressLocality":"Ventura","addressRegion":"CA","postalCode":"93004","streetAddress":"10767 Bridgeport Walk"},"geo":{"@type":"GeoCoordinates","latitude":"34.291715","longitude":"-119.159572"},"itemCondition":"NewCondition","offers":{"@type":"Offer","isFamilyFriendly":true,"areaServed":"Ventura","availability":"LimitedAvailability","offeredBy":"Shea Homes","price":"1154785","priceCurrency":"USD"},"telephone":"888-237-4749","accommodationCategory":"Single Family Residence"}
        """
        # Get the url from the listing_data
        url = listing_data.get("url")
        # Get the name from the listing_data
        name = listing_data.get("name")

        # Validate required fields
        if not url or not name:
            logging.warning(f"🚨 POTENTIAL NEW FORMAT DETECTED - Missing required fields for listing ID:")
            logging.warning(f"   📋 URL: {url}")
            logging.warning(f"   📋 Name: {name}")
            logging.warning(f"   📋 Available keys in data: {list(listing_data.keys())}")
            logging.warning(f"   📋 Data structure: {listing_data}")
            logging.warning(f"   ❌ SKIPPING DATABASE INSERT - Investigation needed")
            return None  # Do not insert into database

        # Add underscores in spaces between words in name
        name = name.replace(" ", "_")
        primary_key = f"{url}_{name}"
        # Return the url + name
        return primary_key
    
    def nhs_card_parse_html(self, nhs_card):
        """
        Parse the HTML for a nhs card listing with proper error handling
        """
        try:
            # Locate the nhs-c-card__body from the nhs_card
            nhs_card_body = nhs_card.find('div', class_='nhs-c-card__body')
            if not nhs_card_body:
                raise ValueError("No nhs-c-card__body found")

            # Locate the nhs-c-card__content from the nhs_card_body
            nhs_card_content = nhs_card_body.find('div', class_='nhs-c-card__content')
            if not nhs_card_content:
                raise ValueError("No nhs-c-card__content found")
            
            # Get the price from the nhs_card with p class = "nhs-c-card__price"
            price_element = nhs_card_content.find('p', class_='nhs-c-card__price')
            if not price_element:
                raise ValueError("No price element found")
            price = price_element.text.strip()
            
            # Get the name container from the nhs_card
            name_content = nhs_card_content.find('h3', class_='nhs-c-card__housing-name')
            if not name_content:
                raise ValueError("No housing name element found")

            # Get the name from the name_content
            name = name_content.text.strip()
            
            # Get the url from the name_content anchor tag
            url_element = name_content.find('a')
            if not url_element or not url_element.get('href'):
                raise ValueError("No URL found in name element")
            url = url_element.get('href')

            # Get the address with proper data-qa attribute handling
            address_element = nhs_card_content.find('p', class_='nhs-c-card__facts', attrs={'data-qa': 'listing_address'})
            if not address_element:
                # Fallback: try to find any nhs-c-card__facts element
                address_element = nhs_card_content.find('p', class_='nhs-c-card__facts')
                if not address_element:
                    raise ValueError("No address element found")
            address = address_element.text.strip()

            return price, name, url, address
            
        except Exception as e:
            logging.debug(f"HTML parsing error: {e}")
            # Try to extract what we can from data attributes as fallback
            return self._extract_from_data_attributes(nhs_card)
    
    def _extract_from_data_attributes(self, nhs_card):
        """
        Fallback method to extract data from card data attributes
        """
        try:
            # Extract from data attributes
            name = nhs_card.get('data-community-name', '')
            price_low = nhs_card.get('data-price-low', '')
            price_high = nhs_card.get('data-price-high', '')
            city = nhs_card.get('data-city', '')
            state = nhs_card.get('data-state-abbreviation', '')
            
            # Construct price range if available
            if price_low and price_high:
                price = f"${int(price_low):,} - ${int(price_high):,}"
            elif price_low:
                price = f"${int(price_low):,}"
            else:
                price = "Price not available"
            
            # Construct address
            address = f"{city}, {state}" if city and state else "Address not available"
            
            # Try to find URL in any anchor tag
            url_element = nhs_card.find('a')
            url = url_element.get('href') if url_element else None
            
            if name and url:
                logging.debug(f"Successfully extracted from data attributes: {name}")
                return price, name, url, address
            else:
                raise ValueError("Insufficient data in attributes")
                
        except Exception as e:
            logging.debug(f"Data attribute extraction failed: {e}")
            raise ValueError("All parsing methods failed")

    def _parse_single_card(self, nhs_card, source_url: str, card_index: int) -> dict:
        """Parse a single nhs card using JSON-LD first, then fallback to HTML."""
        
        # Try JSON-LD parsing first (preferred method)
        json_ld_data = self._extract_json_ld_data(nhs_card)
        if json_ld_data:
            return self._create_document_from_json_ld(json_ld_data, source_url)
        
        # Fallback to HTML parsing
        html_data = self._extract_html_data(nhs_card)
        if html_data:
            return self._create_document_from_html(html_data, source_url)
        logging.warning(f"⚠️ No valid data found in card {card_index}, {nhs_card}")
        return None

    def _extract_json_ld_data(self, nhs_card) -> dict:
        """Extract and validate JSON-LD data from card."""
        scripts = nhs_card.find_all('script', type='application/ld+json')
        
        for script in scripts:
            try:
                data = json.loads(script.text)
                # Validate required fields
                if data.get("url") and data.get("name"):
                    return data
            except json.JSONDecodeError:
                continue
        
        return None

    def _extract_html_data(self, nhs_card) -> dict:
        """Extract data from HTML structure when JSON-LD is not available."""
        try:
            price, name, url, address = self.nhs_card_parse_html(nhs_card)
            
            # Validate required fields
            if url and name:
                return {
                    "price": price,
                    "name": name, 
                    "url": url,
                    "address": address
                }
        except Exception as e:
            logging.debug(f"HTML parsing failed: {e}")
        
        return None

    def _create_document_from_json_ld(self, data: dict, source_url: str) -> dict:
        """Create standardized document from JSON-LD data."""
        listing_id = self.get_script_listing_id(data)
        if not listing_id:
            return None
            
        return {
            "listing_id": listing_id,
            "scraped_at": datetime.now(),
            "source_url": source_url,
            "property_data": data,
            "data_source": "json_ld"
        }

    def _create_document_from_html(self, data: dict, source_url: str) -> dict:
        """Create standardized document from HTML data."""
        listing_id = self.get_nhs_card_listing_id(data)
        if not listing_id:
            return None
            
        return {
            "listing_id": listing_id,
            "scraped_at": datetime.now(), 
            "source_url": source_url,
            "property_data": data,
            "data_source": "html_fallback"
        }

    def _log_found_urls(self, extracted_data):
        """Log the URLs found during parsing for debugging purposes."""
        found_urls = []
        for item in extracted_data:
            listing_id = item.get("listing_id")
            # Extract URL from listing_id format (URL_Name)
            if listing_id and "_" in listing_id:
                potential_url = listing_id.split("_")[0]
                if potential_url.startswith('http'):
                    found_urls.append(potential_url)
        
        if found_urls:
            logging.info(f"🔗 FOUND LISTING URLs:")
            for found_url in found_urls[:5]:  # Show first 5
                logging.info(f"   🏠 {found_url}")
            if len(found_urls) > 5:
                logging.info(f"   ... and {len(found_urls) - 5} more listings")

    async def parse_html(self, url:str, html:str, location_info:dict=None):
        """Parse HTML and extract listing data with proper error handling."""
        try:
            logging.info(f"🔄 Parsing {url}")
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find all nhs-c-card--housing divs
            nhs_cards = soup.find_all('div', class_='nhs-c-card--housing')
            
            if not nhs_cards:
                logging.warning(f"⚠️ No nhs-c-card--housing divs found for {url}")
                return False

            extracted_data = []
            
            # Process each card individually
            for card_index, nhs_card in enumerate(nhs_cards):
                try:
                    document = self._parse_single_card(nhs_card, url, card_index)
                    if document:
                        extracted_data.append(document)
                        logging.info(f"✅ Found listing: {document['listing_id']} (source: {document['data_source']})")
                except Exception as e:
                    logging.warning(f"⚠️ Error parsing card {card_index}: {e}")
                    continue
            
            if not extracted_data:
                logging.warning(f"⚠️ No valid listings found for {url}")
                return False
            
            # Show URLs found during parsing
            self._log_found_urls(extracted_data)
                
            # Process batch and return results
            scraped_ids = await self.process_listings_batch(extracted_data)
            logging.info(f"✅ Parsed {url}: {len(extracted_data)} listings processed")
            return scraped_ids if scraped_ids else set()
            
        except Exception as e:
            logging.error(f"❌ Error parsing {url}: {e}")
            return False
    
    async def _consolidate_price_history(self, listing_id: str):
        """Consolidate price history to permanent storage when archiving"""
        try:
            price_tracker = PriceTracker()
            await price_tracker.connect_to_mongodb()
            await price_tracker.consolidate_to_permanent_storage(listing_id)
            await price_tracker.archive_community_data(listing_id)
            price_tracker.close_connection()
            logging.info(f"✅ Price history consolidated for {listing_id}")
        except Exception as e:
            logging.error(f"❌ Error consolidating price history for {listing_id}: {e}")

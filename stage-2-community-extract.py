import asyncio
import curl_cffi
from bs4 import BeautifulSoup
import os
import json
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import time
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from price_tracker import PriceTracker

load_dotenv()

def get_property_data_from_mongodb():
    """Fetch _id, listing_id, and URL from homepagedata collection."""
    try:
        uri = os.getenv("MONGO_DB_URI")
        client = MongoClient(uri, server_api=ServerApi('1'))
        db = client['newhomesource']
        collection = db['homepagedata']
        
        property_data = {}
        cursor = collection.find({}, {"_id": 1, "listing_id": 1, "property_data.url": 1})
        for doc in cursor:
            if "_id" in doc and "property_data" in doc and "url" in doc["property_data"]:
                property_data[doc["_id"]] = {
                    "url": doc["property_data"]["url"],
                    "listing_id": doc.get("listing_id")  # Get listing_id from homepagedata
                }
        
        client.close()
        return property_data
    except Exception as e:
        print(f"Error fetching property data: {e}")
        return {}

california_rotating_proxy = {
    'https': 'http://35d146gk4kq9otn-country-us-state-california:z51vlpkz84emlb9@rp.scrapegw.com:6060'
}

class AsyncProxyHandler:
    def __init__(self, max_concurrent=10, delay_between_requests=np.random.uniform(0.5, 1.5), max_retries=3):
        self.max_concurrent = max_concurrent
        self.delay_between_requests = delay_between_requests
        self.max_retries = max_retries
        self.semaphore = asyncio.Semaphore(max_concurrent)
        # Browser impersonation options (chrome used for initial attempts, others for failures)
        self.impersonations = ["chrome_android", "safari_ios", "chrome", "safari", "firefox"]
        # Track the current working browser
        self.current_working_browser = "chrome"  # Start with chrome
        self.browser_success_count = 0
        self.browser_lock = asyncio.Lock()
        
    async def connect_to_mongodb(self):
        """Async MongoDB connection"""
        try:
            uri = os.getenv("MONGO_DB_URI")
            client = AsyncIOMotorClient(uri)
            db = client['newhomesource']
            homepagedata_collection = db['homepagedata']
            communitydata_collection = db['communitydata']
            temp_collection = db['temphtml']
            return client, homepagedata_collection, communitydata_collection, temp_collection
        except Exception as e:
            raise Exception(f"Failed to connect to MongoDB: {e}")

    async def fetch_with_proxy(self, url: str, impersonation: str = "auto", max_retries: int = 3) -> Tuple[int, str]:
        """Async HTTP request that sticks with successful browser, rotates only on failures"""
        async with self.semaphore:  # Limit concurrent requests
            last_exception = None
            
            # Use current working browser if "auto", otherwise use specified
            if impersonation == "auto":
                async with self.browser_lock:
                    current_impersonation = self.current_working_browser
            else:
                current_impersonation = impersonation
            
            # Track which browsers we've tried for this request
            browsers_tried = []
            impersonation_idx = self.impersonations.index(current_impersonation) if current_impersonation in self.impersonations else 0
            
            for attempt in range(max_retries + 1):  # +1 for initial attempt
                try:
                    # Add delay between requests for rate limiting
                    if attempt > 0:
                        # Exponential backoff: 2^attempt seconds (2, 4, 8...)
                        backoff_delay = min(2 ** attempt, 30)  # Cap at 30 seconds
                        logging.info(f"🔄 Retry {attempt} for {url}, waiting {backoff_delay}s...")
                        await asyncio.sleep(backoff_delay)
                    else:
                        await asyncio.sleep(self.delay_between_requests)
                    
                    browsers_tried.append(current_impersonation)
                    
                    # Use simple curl_cffi.get method
                    response = await asyncio.to_thread(
                        curl_cffi.get,
                        url,
                        impersonate=current_impersonation,
                        proxies=california_rotating_proxy,
                        timeout=30,
                        verify=False
                    )
                    
                    logging.info(f"🌐 Fetched {url} → Status: {response.status_code} (using {current_impersonation} + proxy)")
                    
                    # Success for any 2xx status code
                    if 200 <= response.status_code < 300:
                        # Update working browser if this was successful
                        if impersonation == "auto" and current_impersonation != self.current_working_browser:
                            async with self.browser_lock:
                                self.current_working_browser = current_impersonation
                                self.browser_success_count = 1
                                logging.info(f"🎯 Updated working browser to {current_impersonation}")
                        elif impersonation == "auto":
                            async with self.browser_lock:
                                self.browser_success_count += 1
                        
                        if attempt > 0:
                            logging.info(f"✅ Request succeeded on attempt {attempt + 1} for {url} with {current_impersonation}")
                        return response.status_code, response.text
                    
                    # Handle specific error codes - rotate browser on failure
                    elif response.status_code in [403, 429, 500, 502, 503, 504]:
                        logging.warning(f"⚠️ HTTP {response.status_code} for {url} with {current_impersonation} (attempt {attempt + 1})")
                        if attempt < max_retries:
                            # Rotate to next browser impersonation on failure
                            impersonation_idx = (impersonation_idx + 1) % len(self.impersonations)
                            current_impersonation = self.impersonations[impersonation_idx]
                            logging.info(f"🔄 Switching to {current_impersonation} for next attempt")
                        else:
                            return response.status_code, ""
                        continue
                    else:
                        # Don't retry for other 4xx errors (404, 400, etc.)
                        logging.error(f"❌ HTTP {response.status_code} error for {url} with {current_impersonation} - not retrying")
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
            logging.error(f"❌ All {max_retries + 1} attempts failed for {url} (tried browsers: {browsers_tried})")
            if last_exception:
                logging.error(f"❌ Last exception: {last_exception}")
            return 0, ""

    # Legacy method - no longer used in individual processing mode
    async def process_single_url(self, temp_collection, _id: str, url: str,
                               impersonation: str = "chrome") -> bool:
        """Legacy: Process a single URL (replaced by individual processing)"""
        status_code, html = await self.fetch_with_proxy(url, impersonation, self.max_retries)
        
        if status_code == 200 and html:
            try:
                await temp_collection.insert_one({"_id": _id, "html": html})
                logging.info(f"✅ Successfully saved HTML for {url}")
                return True
            except Exception as e:
                logging.error(f"DB insert failed for {_id}: {e}")
                return False
        else:
            logging.warning(f"❌ Failed to fetch {url}, final status: {status_code}")
            return False

    async def retry_with_different_impersonation(self, temp_collection, _id: str, url: str) -> bool:
        """Try different browser impersonations for failed requests using curl_cffi"""
        impersonations = ["chrome", "safari", "firefox", "chrome_android", "safari_ios"]
        
        for impersonation in impersonations:
            logging.info(f"🔄 Retrying {url} with {impersonation}")
            success = await self.process_single_url(temp_collection, _id, url, impersonation)
            if success:
                return True
            await asyncio.sleep(2)  # Brief delay between retries
        
        return False

    async def persistent_retry_with_backoff(self, temp_collection, failed_urls, max_duration_minutes=30):
        """Persistent retry mechanism with exponential backoff for ultimate failures"""
        start_time = time.time()
        max_duration = max_duration_minutes * 60  # Convert to seconds
        
        impersonations = ["chrome", "safari", "firefox", "chrome_android", "safari_ios"]
        remaining_urls = failed_urls.copy()
        attempt = 0
        
        logging.info(f"🔄 Starting persistent retry for {len(remaining_urls)} URLs (max {max_duration_minutes} minutes)")
        
        while remaining_urls and (time.time() - start_time) < max_duration:
            attempt += 1
            # Exponential backoff: 30s, 60s, 120s, 240s, 480s, then cap at 10 minutes
            backoff_delay = min(30 * (2 ** (attempt - 1)), 600)
            
            # Rotate impersonation for this attempt
            current_impersonation = impersonations[(attempt - 1) % len(impersonations)]
            
            logging.info(f"🔄 Persistent retry attempt {attempt} using {current_impersonation} (backoff: {backoff_delay}s)")
            
            if attempt > 1:  # Don't wait before first attempt
                await asyncio.sleep(backoff_delay)
            
            # Check if we still have time
            if (time.time() - start_time) >= max_duration:
                logging.warning(f"⏰ 30-minute timeout reached, stopping persistent retry")
                break
            
            # Try all remaining URLs with current impersonation
            retry_tasks = []
            for _id, url in remaining_urls:
                task = self.process_single_url(temp_collection, _id, url, current_impersonation)
                retry_tasks.append((_id, url, task))
            
            # Execute all retries for this attempt
            results = await asyncio.gather(*[task for _, _, task in retry_tasks], return_exceptions=True)
            
            # Remove successful URLs from remaining list
            new_remaining = []
            for (_id, url, _), result in zip(retry_tasks, results):
                if isinstance(result, Exception) or not result:
                    new_remaining.append((_id, url))
                else:
                    logging.info(f"✅ Persistent retry success: {url}")
            
            success_count = len(remaining_urls) - len(new_remaining)
            logging.info(f"📊 Attempt {attempt}: {success_count}/{len(remaining_urls)} succeeded, {len(new_remaining)} remaining")
            
            remaining_urls = new_remaining
            
            # If all succeeded, break early
            if not remaining_urls:
                logging.info(f"🎉 All URLs successfully fetched after {attempt} persistent attempts!")
                break
        
        # Final summary
        elapsed_time = time.time() - start_time
        total_failed = len(failed_urls)
        final_remaining = len(remaining_urls)
        total_succeeded = total_failed - final_remaining
        
        logging.info(f"📊 Persistent retry summary ({elapsed_time:.1f}s):")
        logging.info(f"   ✅ Succeeded: {total_succeeded}/{total_failed}")
        logging.info(f"   ❌ Still failed: {final_remaining}/{total_failed}")
        
        if remaining_urls:
            logging.warning(f"⚠️ {len(remaining_urls)} URLs permanently failed after 30-minute retry:")
            for _id, url in remaining_urls[:5]:  # Log first 5 failed URLs
                logging.warning(f"   💀 {url}")
            if len(remaining_urls) > 5:
                logging.warning(f"   ... and {len(remaining_urls) - 5} more")

    async def reach_target_property_urls_async(self, property_data: Dict[str, Dict]):
        """Main async function to process URLs individually with change tracking"""
        client, homepagedata_collection, communitydata_collection, temp_collection = await self.connect_to_mongodb()
        
        try:
            logging.info(f"🚀 Processing {len(property_data)} URLs individually with smart browser rotation (chrome first, rotate on failures)")
            
            # Get existing community data for change detection
            existing_community_data = await self._get_existing_community_data(communitydata_collection)
            logging.info(f"📊 Found {len(existing_community_data)} existing community records")
            
            # Track changes
            change_stats = {
                "new_communities": 0,
                "updated_communities": 0,
                "unchanged_communities": 0,
                "new_listings": 0,
                "updated_listings": 0,
                "unchanged_listings": 0
            }
            
            success_count = 0
            error_count = 0
            failed_urls = []
            processed_listing_ids = set()
            
            # Process each URL individually
            for idx, (_id, data) in enumerate(property_data.items(), 1):
                url = data['url']
                listing_id = data.get('listing_id')
                
                if not listing_id:
                    logging.warning(f"⚠️ No listing_id found for {_id}")
                    error_count += 1
                    continue
                
                processed_listing_ids.add(listing_id)
                
                # Use "auto" to leverage the working browser
                initial_impersonation = "auto"
                
                logging.info(f"📍 Processing {idx}/{len(property_data)}: {url} (starting with {initial_impersonation})")
                
                try:
                    # Step 1: Fetch HTML
                    status_code, html = await self.fetch_with_proxy(url, initial_impersonation, self.max_retries)
                    
                    if status_code != 200 or not html:
                        logging.warning(f"⚠️ Failed to fetch {url} (status: {status_code})")
                        failed_urls.append((_id, url, listing_id))
                        error_count += 1
                        continue
                    
                    # Step 2: Save HTML to temphtml collection
                    await temp_collection.insert_one({"_id": _id, "url": url, "html": html})
                    logging.debug(f"💾 Saved HTML to temphtml for {url}")
                    
                    # Step 3: Parse HTML from temphtml
                    extracted_info = await self.extract_community_info_async(html, _id)
                    communities = extracted_info.get("communities", [])
                    
                    if not communities:
                        logging.warning(f"⚠️ No communities found for {url}")
                        # Keep HTML in temphtml for debugging, don't delete yet
                        error_count += 1
                        continue
                    
                    # Step 4: Store data to communitydata + detect changes
                    listing_change_type, community_changes = await self._process_community_changes(
                        listing_id, communities, existing_community_data.get(listing_id), communitydata_collection
                    )
                    
                    # Step 5: Clean up temphtml for this successful URL
                    await temp_collection.delete_one({"_id": _id})
                    logging.debug(f"🧹 Cleaned temphtml for {url}")
                    
                    # Update change statistics
                    if listing_change_type == "new":
                        change_stats["new_listings"] += 1
                        change_stats["new_communities"] += len(communities)
                    elif listing_change_type == "updated":
                        change_stats["updated_listings"] += 1
                        change_stats["new_communities"] += community_changes.get("new", 0)
                        change_stats["updated_communities"] += community_changes.get("updated", 0)
                        change_stats["unchanged_communities"] += community_changes.get("unchanged", 0)
                    else:  # unchanged
                        change_stats["unchanged_listings"] += 1
                        change_stats["unchanged_communities"] += len(communities)
                    
                    success_count += 1
                    logging.info(f"✅ Processed {url}: {len(communities)} communities ({listing_change_type})")
                    
                    # Rate limiting delay
                    if idx < len(property_data):
                        await asyncio.sleep(self.delay_between_requests)
                    
                except Exception as e:
                    logging.error(f"❌ Error processing {url}: {e}")
                    # Clean up temphtml on error
                    try:
                        await temp_collection.delete_one({"_id": _id})
                    except:
                        pass
                    failed_urls.append((_id, url, listing_id))
                    error_count += 1
            
            # Handle failed URLs with retry mechanism
            if failed_urls:
                logging.info(f"🔄 Retrying {len(failed_urls)} failed URLs individually...")
                await self._retry_failed_urls_individually(failed_urls, communitydata_collection)
            
            # Detect removed listings (exist in DB but not in current scrape)
            await self._handle_removed_community_listings(
                existing_community_data.keys(), processed_listing_ids, communitydata_collection
            )
            
            # Log comprehensive change summary
            logging.info(f"📊 STAGE 2 CHANGE SUMMARY:")
            logging.info(f"   🆕 New listings: {change_stats['new_listings']}")
            logging.info(f"   🔄 Updated listings: {change_stats['updated_listings']}")  
            logging.info(f"   ✅ Unchanged listings: {change_stats['unchanged_listings']}")
            logging.info(f"   🏠 New communities: {change_stats['new_communities']}")
            logging.info(f"   📝 Updated communities: {change_stats['updated_communities']}")
            logging.info(f"   📊 Unchanged communities: {change_stats['unchanged_communities']}")
            logging.info(f"   ✅ Success: {success_count}, ❌ Errors: {error_count}")
            
            # Capture price snapshots after all processing
            await self._capture_price_snapshots()
            
        finally:
            client.close()

    async def extract_community_info_async(self, html: str, _id: str) -> Dict:
        """Async version of HTML extraction (CPU-bound, but can be made async)"""
        # Use asyncio.to_thread for CPU-bound operations in Python 3.9+
        return await asyncio.to_thread(self._extract_sync, html, _id)
    
    def generate_community_id(self, name: str, url: str) -> str:
        """Generate unique community ID from name and URL"""
        if not name or not url:
            return None
        # Clean name for ID format
        clean_name = name.replace(" ", "_").replace(",", "").replace(".", "")
        return f"{url}_{clean_name}"

    def _extract_sync(self, html: str, homepage_id: str) -> Dict:
        """Extract data from multiple housing cards on the page"""
        extracted_info = {"homepage_id": homepage_id, "communities": []}
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find all housing card containers - try multiple selectors
        housing_cards = soup.find_all("div", class_="nhs-c-card--housing")
        if not housing_cards:
            # Fallback: try without the -- in class name
            housing_cards = soup.find_all("div", class_="nhs-c-card-housing")
        if not housing_cards:
            # Try partial class match
            housing_cards = soup.find_all("div", class_=lambda x: x and "nhs-c-card" in x and "housing" in x)
        
        logging.debug(f"Found {len(housing_cards)} housing cards in HTML")
        
        for card_index, card in enumerate(housing_cards):
            community_data = {}
            
            # Extract build statuses from this card
            status_div = card.find("div", class_="nhs-c-card__statuses")
            if status_div:
                build_statuses = []
                for span in status_div.find_all("span"):
                    text = span.get_text(strip=True)
                    if text:
                        build_statuses.append(text)
                if build_statuses:
                    community_data["build_status"] = build_statuses
            
            # Extract JSON-LD scripts from this card
            scripts = card.find_all("script", type="application/ld+json")
            if not scripts:
                # Try fallback parsing if no scripts found
                fallback_data = self._fallback_parse_card(card)
                if fallback_data:
                    community_data.update(fallback_data)
            else:
                for script in scripts:
                    try:
                        if script.string:
                            data = json.loads(script.string.strip())
                            if data.get("@type") == "SingleFamilyResidence":
                                community_data.update({
                                    "name": data.get("name"),
                                    "url": data.get("url"),
                                    "image": data.get("image"),
                                    "build_type": self._determine_build_type(data.get("url", ""))
                                })
                            elif data.get("@type") == "Product":
                                offers = data.get("offers", {})
                                community_data.update({
                                    "price": offers.get("price"),
                                    "price_currency": offers.get("priceCurrency")
                                })
                    except (json.JSONDecodeError, AttributeError, TypeError) as e:
                        logging.debug(f"Failed to parse JSON-LD: {e}")
                        continue
            
            # Generate unique community ID and add to data if we have name/url
            if community_data.get("name") and community_data.get("url"):
                community_id = self.generate_community_id(community_data["name"], community_data["url"])
                if community_id:
                    community_data["community_id"] = community_id
                    community_data["card_index"] = card_index
                    extracted_info["communities"].append(community_data)
        
        # Add summary info
        extracted_info["total_communities_found"] = len(extracted_info["communities"])
        
        # Debug logging
        if not extracted_info["communities"]:
            logging.warning(f"No communities extracted. Found {len(housing_cards)} housing cards")
            # Log first few div classes for debugging
            all_divs = soup.find_all("div", class_=True)[:10]
            logging.debug(f"Sample div classes: {[div.get('class') for div in all_divs]}")
        
        return extracted_info
    
    def _determine_build_type(self, url: str) -> str:
        """Determine build type from URL"""
        if "spec" in url.lower():
            return "spec"
        elif "plan" in url.lower():
            return "plan"
        return "unknown"
    
    async def _retry_failed_urls_individually(self, failed_urls: List[Tuple], communitydata_collection):
        """Retry failed URLs individually with different browser impersonations"""
        impersonations = ["safari", "firefox", "chrome_android", "safari_ios"]
        
        retry_success = 0
        final_failures = 0
        
        for _id, url, listing_id in failed_urls:
            success = False
            
            # Try each impersonation for this URL
            for impersonation in impersonations:
                try:
                    logging.info(f"🔄 Retrying {url} with {impersonation}")
                    
                    # Fetch with different browser
                    status_code, html = await self.fetch_with_proxy(url, impersonation, self.max_retries)
                    
                    if status_code == 200 and html:
                        # Follow same flow: Save HTML → Parse → Store
                        # Note: temp_collection not available in retry, so parse directly
                        extracted_info = await self.extract_community_info_async(html, _id)
                        communities = extracted_info.get("communities", [])
                        
                        if communities and listing_id:
                            community_doc = {
                                "listing_id": listing_id,
                                "scraped_at": datetime.now().isoformat(),
                                "community_data": {
                                    "communities": communities
                                },
                                "total_communities_found": len(communities)
                            }
                            
                            await communitydata_collection.update_one(
                                {"listing_id": listing_id},
                                {"$set": community_doc},
                                upsert=True
                            )
                            
                            logging.info(f"✅ Retry successful for {url} with {impersonation}: {len(communities)} communities")
                            retry_success += 1
                            success = True
                            break
                    
                    await asyncio.sleep(2)  # Brief delay between retry attempts
                    
                except Exception as e:
                    logging.debug(f"Retry failed for {url} with {impersonation}: {e}")
                    continue
            
            if not success:
                logging.warning(f"❌ All retry attempts failed for {url}")
                final_failures += 1
        
        logging.info(f"📊 Retry summary: {retry_success} recovered, {final_failures} permanently failed")
    
    async def _get_existing_community_data(self, communitydata_collection):
        """Get existing community data for change detection"""
        try:
            existing_data = {}
            cursor = communitydata_collection.find({}, {"listing_id": 1, "community_data": 1, "scraped_at": 1})
            
            async for doc in cursor:
                listing_id = doc.get("listing_id")
                if listing_id:
                    existing_data[listing_id] = {
                        "community_data": doc.get("community_data", {}),
                        "scraped_at": doc.get("scraped_at"),
                        "communities": doc.get("community_data", {}).get("communities", [])
                    }
            
            return existing_data
        except Exception as e:
            logging.error(f"❌ Error getting existing community data: {e}")
            return {}
    
    async def _process_community_changes(self, listing_id: str, new_communities: List[Dict], 
                                       existing_data: Dict, communitydata_collection):
        """Process community changes and return change type and statistics"""
        try:
            if not existing_data:
                # New listing - save all communities
                community_doc = {
                    "listing_id": listing_id,
                    "scraped_at": datetime.now().isoformat(),
                    "community_data": {
                        "communities": new_communities
                    },
                    "total_communities_found": len(new_communities),
                    "listing_status": "new",
                    "last_updated": datetime.now()
                }
                
                await communitydata_collection.update_one(
                    {"listing_id": listing_id},
                    {"$set": community_doc},
                    upsert=True
                )
                
                return "new", {"new": len(new_communities)}
            
            # Compare existing vs new communities
            existing_communities = existing_data.get("communities", [])
            changes = self._detect_community_changes(existing_communities, new_communities)
            
            if changes["has_changes"]:
                # Update with changes
                community_doc = {
                    "listing_id": listing_id,
                    "scraped_at": datetime.now().isoformat(),
                    "community_data": {
                        "communities": new_communities
                    },
                    "total_communities_found": len(new_communities),
                    "listing_status": "updated", 
                    "last_updated": datetime.now(),
                    "previous_scraped_at": existing_data.get("scraped_at"),
                    "change_summary": {
                        "new_communities": changes["new_count"],
                        "updated_communities": changes["updated_count"],
                        "removed_communities": changes["removed_count"],
                        "total_changes": changes["total_changes"]
                    }
                }
                
                await communitydata_collection.update_one(
                    {"listing_id": listing_id},
                    {"$set": community_doc},
                    upsert=True
                )
                
                logging.info(f"🔄 {listing_id}: {changes['total_changes']} changes detected")
                return "updated", {
                    "new": changes["new_count"],
                    "updated": changes["updated_count"], 
                    "unchanged": len(new_communities) - changes["new_count"] - changes["updated_count"]
                }
            else:
                # No changes - just update timestamp
                await communitydata_collection.update_one(
                    {"listing_id": listing_id},
                    {"$set": {
                        "scraped_at": datetime.now().isoformat(),
                        "listing_status": "active"
                    }}
                )
                
                return "unchanged", {"unchanged": len(new_communities)}
                
        except Exception as e:
            logging.error(f"❌ Error processing community changes for {listing_id}: {e}")
            return "error", {}
    
    def _detect_community_changes(self, existing_communities: List[Dict], new_communities: List[Dict]) -> Dict:
        """Detect changes between existing and new community data"""
        try:
            # Create lookups by community_id
            existing_lookup = {c.get("community_id"): c for c in existing_communities if c.get("community_id")}
            new_lookup = {c.get("community_id"): c for c in new_communities if c.get("community_id")}
            
            existing_ids = set(existing_lookup.keys())
            new_ids = set(new_lookup.keys())
            
            # Detect changes
            new_community_ids = new_ids - existing_ids
            removed_community_ids = existing_ids - new_ids
            common_community_ids = existing_ids & new_ids
            
            updated_count = 0
            
            # Check for updates in common communities
            for community_id in common_community_ids:
                existing_community = existing_lookup[community_id]
                new_community = new_lookup[community_id]
                
                if self._has_community_changed(existing_community, new_community):
                    updated_count += 1
            
            total_changes = len(new_community_ids) + len(removed_community_ids) + updated_count
            
            return {
                "has_changes": total_changes > 0,
                "new_count": len(new_community_ids),
                "removed_count": len(removed_community_ids),
                "updated_count": updated_count,
                "total_changes": total_changes,
                "new_ids": list(new_community_ids),
                "removed_ids": list(removed_community_ids)
            }
            
        except Exception as e:
            logging.error(f"❌ Error detecting community changes: {e}")
            return {"has_changes": False, "new_count": 0, "removed_count": 0, "updated_count": 0, "total_changes": 0}
    
    def _has_community_changed(self, existing_community: Dict, new_community: Dict) -> bool:
        """Check if individual community has changed"""
        # Compare key fields that indicate changes
        compare_fields = ["name", "price", "build_status", "build_type", "url"]
        
        for field in compare_fields:
            existing_value = existing_community.get(field)
            new_value = new_community.get(field)
            
            # Normalize for comparison
            if field == "price":
                try:
                    existing_value = float(existing_value) if existing_value else 0
                    new_value = float(new_value) if new_value else 0
                except:
                    pass
            
            if existing_value != new_value:
                return True
        
        return False
    
    async def _handle_removed_community_listings(self, existing_listing_ids, processed_listing_ids, communitydata_collection):
        """Handle community listings that were removed (exist in DB but not in current scrape)"""
        try:
            removed_listing_ids = set(existing_listing_ids) - processed_listing_ids
            
            if not removed_listing_ids:
                logging.info("✅ No removed community listings detected")
                return
            
            # Safety check - prevent mass removal due to scraping issues
            removal_percentage = len(removed_listing_ids) / len(existing_listing_ids) if existing_listing_ids else 0
            
            if removal_percentage > 0.5:  # >50% would be removed
                logging.error(f"🚨 SAFETY CHECK: {len(removed_listing_ids)}/{len(existing_listing_ids)} community listings appear removed ({removal_percentage:.1%})")
                logging.error("🚨 This may indicate a scraping issue - skipping mass removal")
                logging.error("🚨 Manual investigation required")
                return
            
            logging.info(f"🗑️ Archiving {len(removed_listing_ids)} removed community listings")
            
            # Mark as archived (don't delete - keep for historical analysis)
            for listing_id in removed_listing_ids:
                await communitydata_collection.update_one(
                    {"listing_id": listing_id},
                    {"$set": {
                        "is_archived": True,
                        "archived_at": datetime.now(),
                        "archive_reason": "missing from current Stage 2 scrape",
                        "listing_status": "archived"
                    }}
                )
                
                logging.info(f"📦 Archived community listing: {listing_id}")
            
            logging.info(f"📦 Successfully archived {len(removed_listing_ids)} community listings")
            
        except Exception as e:
            logging.error(f"❌ Error handling removed community listings: {e}")
    
    def _fallback_parse_card(self, card_element):
        """Fallback parsing when JSON-LD scripts are not available"""
        # TODO: Implement fallback parsing logic
        # This function should extract data when script tags don't exist
        # Placeholder for your pattern identification and implementation
        logging.debug("Fallback parsing needed - implement pattern identification")
        return None
    
    def html_parse_fallback(self, html: str, community_id: str):
        """Parse HTML and extract community data"""
        try:
            # Parse HTML and extract community data
            soup = BeautifulSoup(html, 'html.parser')
            # Extract community data from soup
            return soup
        except Exception as e:
            logging.error(f"❌ Error parsing HTML: {e}")
            return None

    async def _extract_from_temp_collection(self, communitydata_collection, temp_collection, property_data):
        """LEGACY: Extract data from temp collection (replaced by individual processing)"""
        try:
            # Fetch all documents from temp collection
            cursor = temp_collection.find({})
            documents = await cursor.to_list(length=None)
         
            if not documents:
                logging.warning("⚠️ No HTML documents found in temp collection")
                return
            
            # Create extraction tasks
            extraction_tasks = []
            for doc in documents:
                community_id = doc["_id"]  # This is the homepagedata _id
                html = doc["html"]
                task = self.extract_community_info_async(html, community_id)
                extraction_tasks.append((community_id, task))
            
            # Process all extractions concurrently
            logging.info(f"🔍 Extracting community data from {len(extraction_tasks)} HTML documents...")
            
            successful_extractions = 0
            total_communities_saved = 0
            
            for community_id, task in extraction_tasks:
                try:
                    extracted_info = await task
                    communities = extracted_info.get("communities", [])
                    
                    if communities:
                        # Get listing_id from homepagedata for this homepage_id
                        listing_id = None
                        for data_id, data in property_data.items():
                            if data_id == community_id:
                                listing_id = data.get('listing_id')
                                break
                        
                        if not listing_id:
                            logging.warning(f"⚠️ No listing_id found for community {community_id}")
                            continue
                            
                        # Create single document with all communities nested
                        community_doc = {
                            "listing_id": listing_id,
                            "scraped_at": datetime.now().isoformat(),
                            "community_data": {
                                "communities": communities
                            },
                            "total_communities_found": len(communities)
                        }
                        
                        # Upsert based on listing_id to avoid duplicates
                        await communitydata_collection.update_one(
                            {"listing_id": listing_id},
                            {"$set": community_doc},
                            upsert=True
                        )
                        total_communities_saved += len(communities)
                        logging.info(f"✅ Saved document with {len(communities)} communities for listing {listing_id}")
                    else:
                        # Implement html fallback here.
                        html_fallback = self.html_parse_fallback(html, community_id)
                        if html_fallback:
                            logging.info(f"✅ HTML fallback successful for listing {listing_id}")
                        else:
                            logging.warning(f"⚠️ HTML fallback failed for listing {listing_id}")
                        logging.warning(f"⚠️ No communities found for listing {listing_id}")
                    
                    successful_extractions += 1
                    
                except Exception as e:
                    logging.error(f"❌ Extraction failed for {listing_id}: {e}")
            
            logging.info(f"✅ Successfully processed {successful_extractions}/{len(documents)} documents")
            logging.info(f"📊 Total communities saved: {total_communities_saved}")
            
        finally:
            # Always clean up temp collection
            deleted_count = await temp_collection.delete_many({})
            logging.info(f"🧹 Cleaned up {deleted_count.deleted_count} temp documents")
    
    async def _capture_price_snapshots(self):
        """Capture price snapshots after Stage 2 completion"""
        try:
            price_tracker = PriceTracker()
            await price_tracker.connect_to_mongodb()
            await price_tracker.capture_price_snapshots_from_stage2()
            price_tracker.close_connection()
            logging.info("✅ Price snapshots captured successfully")
        except Exception as e:
            logging.error(f"❌ Error capturing price snapshots: {e}")

# Usage example
async def main():
    """Main async execution"""
    logging.basicConfig(level=logging.INFO)
    
    # Initialize handler with concurrency settings
    handler = AsyncProxyHandler(
        max_concurrent=5,  # Process 5 URLs simultaneously (reduced from 10)
        delay_between_requests=0.5,  # 500ms between requests
        max_retries=3  # Retry failed requests up to 3 times
    )
    
    property_data = get_property_data_from_mongodb()

    # Process URLs and extract data (all in one flow)
    await handler.reach_target_property_urls_async(property_data)

if __name__ == "__main__":
    asyncio.run(main())

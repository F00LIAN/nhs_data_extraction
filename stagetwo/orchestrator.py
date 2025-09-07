"""
Stage 2 Orchestrator Module
Coordinates all Stage 2 components for community data extraction
"""

import asyncio
import logging
from typing import Dict, List, Tuple
from .data_fetcher import DataFetcher
from .http_client import HTTPClient
from .html_parser import HTMLParser
from .data_processor import DataProcessor


class Stage2Orchestrator:
    """Orchestrates the complete Stage 2 community extraction process"""
    
    def __init__(self, max_concurrent=5, delay_between_requests=0.5, max_retries=3):
        """
        Input: max_concurrent (int), delay_between_requests (float), max_retries (int)
        Output: None
        Description: Initialize orchestrator with all required components
        """
        self.data_fetcher = DataFetcher()
        self.http_client = HTTPClient(max_concurrent, delay_between_requests, max_retries)
        self.html_parser = HTMLParser()
        self.data_processor = DataProcessor()
        
        # Track processing statistics
        self.stats = {
            "new_listings": 0,
            "updated_listings": 0,
            "unchanged_listings": 0,
            "new_communities": 0,
            "updated_communities": 0,
            "unchanged_communities": 0,
            "success_count": 0,
            "error_count": 0
        }
    
    async def execute_stage2_extraction(self, property_data: Dict = None) -> Dict:
        """
        Execute complete Stage 2 community extraction process.
        
        Input: property_data (Dict, optional) - Pre-filtered property data from routing
        Output: Dict with processing statistics and results
        Description: Main orchestration method that coordinates all Stage 2 components
        """
        try:
            # Step 1: Get property data (either provided or fetch from Stage 1)
            if property_data is None:
                logging.info("🔗 Fetching property data from Stage 1...")
                property_data = self.data_fetcher.get_property_data()
            else:
                logging.info("🔗 Using pre-filtered property data from routing...")
            
            if not property_data:
                logging.error("❌ No property data found for Stage 2")
                return {"success": False, "error": "No Stage 2 data available"}
            
            logging.info(f"✅ Found {len(property_data)} properties to process")
            
            # Step 2: Connect to MongoDB
            client, homepagedata_collection, communitydata_collection, temp_collection = await self.data_processor.connect_to_mongodb()
            
            try:
                # Step 3: Get existing community data for change detection
                existing_community_data = await self.data_processor.get_existing_community_data(communitydata_collection)
                logging.info(f"📊 Found {len(existing_community_data)} existing community records")
                
                # Step 4: Process each property URL
                failed_urls = []
                processed_listing_ids = set()
                
                for idx, (_id, data) in enumerate(property_data.items(), 1):
                    result = await self._process_single_property(
                        _id, data, idx, len(property_data),
                        temp_collection, communitydata_collection,
                        existing_community_data, processed_listing_ids
                    )
                    
                    if not result["success"]:
                        failed_urls.append((_id, data["url"], data.get("listing_id")))
                
                # Step 5: Handle failed URLs with retry
                if failed_urls:
                    await self._retry_failed_urls(failed_urls, communitydata_collection)
                
                # Step 6: Handle removed listings
                await self.data_processor.handle_removed_listings(
                    set(existing_community_data.keys()), processed_listing_ids, communitydata_collection
                )
                
                # Step 7: Capture price snapshots
                await self.data_processor.capture_price_snapshots()
                
                # Step 8: Log final statistics
                self._log_final_statistics()
                
                return {
                    "success": True,
                    "stats": self.stats,
                    "total_processed": len(property_data),
                    "failed_count": len(failed_urls)
                }
                
            finally:
                client.close()
                
        except Exception as e:
            logging.error(f"❌ Stage 2 orchestration failed: {e}")
            return {"success": False, "error": str(e)}
    
    async def _process_single_property(self, _id: str, data: Dict, idx: int, total: int,
                                     temp_collection, communitydata_collection,
                                     existing_community_data: Dict, processed_listing_ids: set) -> Dict:
        """
        Process a single property URL through the complete pipeline.
        
        Input: Property data, collections, tracking sets
        Output: Dict with success status and details
        Description: Processes one property through fetch → parse → store pipeline
        """
        url = data['url']
        listing_id = data.get('listing_id')
        
        if not listing_id:
            logging.warning(f"⚠️ No listing_id found for {_id}")
            self.stats["error_count"] += 1
            return {"success": False, "error": "Missing listing_id"}
        
        processed_listing_ids.add(listing_id)
        logging.info(f"📍 Processing {idx}/{total}: {url}")
        
        try:
            # Step 1: Fetch HTML
            status_code, html = await self.http_client.fetch_url(url)
            
            if status_code != 200 or not html:
                logging.warning(f"⚠️ Failed to fetch {url} (status: {status_code})")
                self.stats["error_count"] += 1
                return {"success": False, "error": f"HTTP {status_code}"}
            
            # Step 2: Save HTML to temp collection
            await temp_collection.insert_one({"_id": _id, "url": url, "html": html})
            
            # Step 3: Parse HTML
            # Extract location data and metadata from the property data
            county = data.get('county')
            address_locality = data.get('addressLocality')
            postal_code = data.get('postalCode')
            offered_by = data.get('offeredBy')
            accommodation_category = data.get('accommodationCategory')
            
            extracted_info = self.html_parser.extract_community_data(
                html=html,
                homepage_id=_id,
                address_locality=address_locality,
                county=county,
                postal_code=postal_code,
                offered_by=offered_by,
                accommodation_category=accommodation_category
            )
            communities = extracted_info.get("communities", [])
            
            if not communities:
                logging.warning(f"⚠️ No communities found for {url}")
                self.stats["error_count"] += 1
                return {"success": False, "error": "No communities found"}
            
            # Step 4: Process changes and update database
            listing_change_type, community_changes = await self.data_processor.process_community_changes(
                listing_id, communities, existing_community_data.get(listing_id), communitydata_collection
            )
            
            # Step 5: Clean up temp HTML
            await temp_collection.delete_one({"_id": _id})
            
            # Step 6: Update statistics
            self._update_statistics(listing_change_type, community_changes, len(communities))
            
            self.stats["success_count"] += 1
            logging.info(f"✅ Processed {url}: {len(communities)} communities ({listing_change_type})")
            
            return {"success": True, "communities": len(communities), "change_type": listing_change_type}
            
        except Exception as e:
            logging.error(f"❌ Error processing {url}: {e}")
            # Clean up temp HTML on error
            try:
                await temp_collection.delete_one({"_id": _id})
            except:
                pass
            self.stats["error_count"] += 1
            return {"success": False, "error": str(e)}
    
    async def _retry_failed_urls(self, failed_urls: List[Tuple], communitydata_collection):
        """
        Retry failed URLs with different browser impersonations.
        
        Input: failed_urls (List[Tuple]), communitydata_collection
        Output: None
        Description: Attempts to recover failed URLs using different browser strategies
        """
        if not failed_urls:
            return
        
        logging.info(f"🔄 Retrying {len(failed_urls)} failed URLs...")
        impersonations = ["safari", "firefox", "chrome_android", "safari_ios"]
        
        retry_success = 0
        
        for _id, url, listing_id in failed_urls:
            success = False
            
            for impersonation in impersonations:
                try:
                    logging.info(f"🔄 Retrying {url} with {impersonation}")
                    
                    # Fetch with specific browser
                    status_code, html = await self.http_client.fetch_url(url, impersonation)
                    
                    if status_code == 200 and html:
                        # Parse and store directly (no temp collection in retry)
                        # Need to fetch homepage data for retry to get location info
                        homepage_data = await self.data_fetcher.get_homepage_data(_id)
                        if not homepage_data:
                            logging.warning(f"⚠️ Could not find homepage data for retry of {url}")
                            continue
                        
                        extracted_info = self.html_parser.extract_community_data(
                            html=html,
                            homepage_id=_id,
                            address_locality=homepage_data.get('property_data', {}).get('address', {}).get('addressLocality'),
                            county=homepage_data.get('property_data', {}).get('address', {}).get('county'),
                            postal_code=homepage_data.get('property_data', {}).get('address', {}).get('postalCode'),
                            offered_by=homepage_data.get('property_data', {}).get('offers', {}).get('offeredBy'),
                            accommodation_category=homepage_data.get('property_data', {}).get('accommodationCategory')
                        )
                        communities = extracted_info.get("communities", [])
                        
                        if communities and listing_id:
                            community_doc = {
                                "listing_id": listing_id,
                                "scraped_at": self._get_current_timestamp(),
                                "community_data": {"communities": communities},
                                "total_communities_found": len(communities)
                            }
                            
                            await communitydata_collection.update_one(
                                {"listing_id": listing_id},
                                {"$set": community_doc},
                                upsert=True
                            )
                            
                            logging.info(f"✅ Retry successful for {url}: {len(communities)} communities")
                            retry_success += 1
                            success = True
                            break
                    
                    await asyncio.sleep(2)  # Brief delay between retry attempts
                    
                except Exception as e:
                    logging.debug(f"Retry failed for {url} with {impersonation}: {e}")
                    continue
            
            if not success:
                logging.warning(f"❌ All retry attempts failed for {url}")
        
        logging.info(f"📊 Retry summary: {retry_success}/{len(failed_urls)} recovered")
    
    def _update_statistics(self, listing_change_type: str, community_changes: Dict, community_count: int):
        """Update processing statistics based on change type"""
        if listing_change_type == "new":
            self.stats["new_listings"] += 1
            self.stats["new_communities"] += community_count
        elif listing_change_type == "updated":
            self.stats["updated_listings"] += 1
            self.stats["new_communities"] += community_changes.get("new", 0)
            self.stats["updated_communities"] += community_changes.get("updated", 0)
            self.stats["unchanged_communities"] += community_changes.get("unchanged", 0)
        else:  # unchanged
            self.stats["unchanged_listings"] += 1
            self.stats["unchanged_communities"] += community_count
    
    def _log_final_statistics(self):
        """Log comprehensive final statistics"""
        logging.info(f"📊 STAGE 2 EXTRACTION COMPLETE:")
        logging.info(f"   🆕 New listings: {self.stats['new_listings']}")
        logging.info(f"   🔄 Updated listings: {self.stats['updated_listings']}")
        logging.info(f"   ✅ Unchanged listings: {self.stats['unchanged_listings']}")
        logging.info(f"   🏠 New communities: {self.stats['new_communities']}")
        logging.info(f"   📝 Updated communities: {self.stats['updated_communities']}")
        logging.info(f"   📊 Unchanged communities: {self.stats['unchanged_communities']}")
        logging.info(f"   ✅ Success: {self.stats['success_count']}, ❌ Errors: {self.stats['error_count']}")
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format"""
        from datetime import datetime
        return datetime.now().isoformat()

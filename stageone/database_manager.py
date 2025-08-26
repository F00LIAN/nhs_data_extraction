"""
Database Manager Module
Handles all MongoDB operations for listings data.

Input: MongoDB connection parameters, listing documents
Output: Database operation results
Description: Centralized database operations with connection management and CRUD operations.
"""

import asyncio
import os
import logging
from datetime import datetime
from typing import Dict, List, Set, Optional
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.client = None
        self.db = None
        self.homepagedata_collection = None
        self.temp_collection = None
        self.archivedlistings_collection = None

    async def connect(self) -> bool:
        """
        Input: None
        Output: bool (connection success)
        Description: Establish async MongoDB connection and ping database
        """
        try:
            uri = os.getenv("MONGO_DB_URI")
            self.client = AsyncIOMotorClient(uri)
            self.db = self.client['newhomesource']
            self.homepagedata_collection = self.db['homepagedata']
            self.temp_collection = self.db['temphtml']
            self.archivedlistings_collection = self.db['archivedlistings']

            await self.client.admin.command('ping')
            logging.info("✅ MongoDB connected successfully")
            return True
        except Exception as e:
            logging.error(f"❌ MongoDB connection failed: {e}")
            return False

    async def get_existing_listing_ids(self) -> Set[str]:
        """
        Input: None
        Output: Set of existing listing IDs
        Description: Retrieve all listing IDs currently in the database
        """
        try:
            return set(await self.homepagedata_collection.distinct("listing_id"))
        except Exception as e:
            logging.error(f"❌ Error getting existing listings: {e}")
            return set()

    async def process_listing_batch(self, extracted_data: List[Dict]) -> Set[str]:
        """
        Input: List of listing documents to process
        Output: Set of processed listing IDs
        Description: Process batch of listings with insert/update logic
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
                existing_doc = await self.homepagedata_collection.find_one(
                    {"listing_id": listing_id},
                    {"listing_id": 1, "scraped_at": 1, "property_data": 1, "data_source": 1}
                )
                
                if existing_doc:
                    needs_update = self._has_listing_changed(existing_doc, document)
                    
                    if needs_update:
                        document["listing_status"] = "updated"
                        document["last_updated"] = datetime.now()
                        document["previous_scraped_at"] = existing_doc.get("scraped_at")
                        
                        await self.homepagedata_collection.replace_one(
                            {"listing_id": listing_id}, document
                        )
                        processed_count["updated"] += 1
                        logging.info(f"🔄 Updated listing: {listing_id}")
                    else:
                        await self.homepagedata_collection.update_one(
                            {"listing_id": listing_id},
                            {"$set": {"scraped_at": datetime.now(), "listing_status": "active"}}
                        )
                        processed_count["unchanged"] += 1
                else:
                    document["listing_status"] = "new"
                    document["first_scraped_at"] = datetime.now()
                    
                    await self.homepagedata_collection.insert_one(document)
                    processed_count["new"] += 1
                    new_listing_ids.append(listing_id)
                    logging.info(f"🆕 New listing: {listing_id}")
                    
            except Exception as e:
                logging.error(f"❌ Error processing listing {listing_id}: {e}")
                
        total_ops = sum(processed_count.values())
        logging.info(f"📊 Processed: {processed_count['new']} new, {processed_count['updated']} updated, {processed_count['unchanged']} unchanged")
        
        if new_listing_ids:
            logging.info(f"🆕 {len(new_listing_ids)} new listings found")
        
        return scraped_listing_ids

    async def archive_missing_listings(self, missing_listing_ids: Set[str]) -> int:
        """
        Input: Set of missing listing IDs
        Output: Number of listings archived
        Description: Archive listings that are no longer available
        """
        if not missing_listing_ids:
            return 0
            
        archived_count = 0
        for listing_id in missing_listing_ids:
            try:
                listing = await self.homepagedata_collection.find_one({"listing_id": listing_id})
                if listing:
                    listing["archived_at"] = datetime.now()
                    listing["archive_reason"] = "missing from current scrape"
                    listing["original_scraped_at"] = listing.get("scraped_at")
                    
                    await self.archivedlistings_collection.insert_one(listing)
                    await self.homepagedata_collection.delete_one({"listing_id": listing_id})
                    
                    archived_count += 1
                    logging.info(f"📦 Archived missing listing: {listing_id}")
                    
            except Exception as e:
                logging.error(f"❌ Failed to archive {listing_id}: {e}")
                
        return archived_count

    async def store_temp_html(self, doc_id: str, url: str, html: str) -> bool:
        """
        Input: Document ID, URL, HTML content
        Output: Success boolean
        Description: Store HTML temporarily for processing
        """
        try:
            await self.temp_collection.insert_one({"_id": doc_id, "url": url, "html": html})
            return True
        except Exception as e:
            logging.error(f"❌ Failed to store temp HTML: {e}")
            return False

    async def cleanup_temp_collection(self) -> int:
        """
        Input: None
        Output: Number of documents deleted
        Description: Clean up temporary HTML storage
        """
        try:
            result = await self.temp_collection.delete_many({})
            logging.info(f"🧹 Cleaned {result.deleted_count} temp documents")
            return result.deleted_count
        except Exception as e:
            logging.warning(f"⚠️ Failed to clean temp collection: {e}")
            return 0

    def _has_listing_changed(self, existing_doc: Dict, new_doc: Dict) -> bool:
        """
        Input: Existing document, new document
        Output: Boolean indicating if data changed
        Description: Compare documents to detect significant changes
        """
        existing_data = existing_doc.get("property_data", {})
        new_data = new_doc.get("property_data", {})
        
        existing_source = existing_doc.get("data_source", "unknown")
        new_source = new_doc.get("data_source", "unknown")
        
        if existing_source != new_source:
            return True
        
        existing_std = self._extract_comparable_values(existing_data, existing_source)
        new_std = self._extract_comparable_values(new_data, new_source)
        
        return existing_std != new_std

    def _extract_comparable_values(self, data: Dict, data_source: str) -> Dict:
        """
        Input: Property data dict, data source type
        Output: Standardized comparison dict
        Description: Extract comparable values for change detection
        """
        if data_source == "json_ld":
            return {
                "name": data.get("name"),
                "url": data.get("url"),
                "price": self._get_nested_value(data, "offers.price"),
                "address": self._standardize_address(data.get("address")),
                "telephone": data.get("telephone")
            }
        else:  # html_fallback
            return {
                "name": data.get("name"),
                "url": data.get("url"), 
                "price": self._standardize_price(data.get("price")),
                "address": self._standardize_address(data.get("address"))
            }

    def _get_nested_value(self, data: Dict, key_path: str):
        """Get nested dict value using dot notation"""
        keys = key_path.split('.')
        value = data
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        return value

    def _standardize_address(self, address_data) -> Optional[str]:
        """Standardize address to string format"""
        if isinstance(address_data, dict):
            parts = []
            for field in ["streetAddress", "addressLocality", "addressRegion", "postalCode"]:
                if address_data.get(field):
                    parts.append(address_data[field])
            return ", ".join(parts)
        return str(address_data) if address_data else None

    def _standardize_price(self, price_data) -> Optional[str]:
        """Standardize price to comparable format"""
        if not price_data:
            return None
        return str(price_data).replace("$", "").replace(",", "").strip()

    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            logging.info("🔌 MongoDB connection closed")

"""
Stage One and Two Check Module
Handles masterplan detection and routing between Stage 1 and Stage 2
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv

load_dotenv()


class StageOneAndTwoCheck:
    """Checks Stage 1 results and routes masterplan vs regular communities"""
    
    def __init__(self):
        self.client = None
        self.db = None
        self.homepagedata_collection = None
        self.homepagedata_archived_collection = None
        self.masterplandata_collection = None
        self.basiccommunitydata_collection = None
        
    async def connect_to_mongodb(self):
        """Connect to MongoDB collections"""
        try:
            uri = os.getenv("MONGO_DB_URI")
            self.client = AsyncIOMotorClient(uri)
            self.db = self.client['newhomesource']
            self.db_archived = self.client['archived']
            
            self.homepagedata_collection = self.db['homepagedata']
            self.homepagedata_archived_collection = self.db_archived['homepagedata_archived']
            self.masterplandata_collection = self.db['masterplandata']
            self.basiccommunitydata_collection = self.db['basiccommunitydata']
            
            # Create indexes for special collections
            await self.masterplandata_collection.create_index([("masterplanlisting_id", 1)])
            await self.basiccommunitydata_collection.create_index([("basic_community_listing_id", 1)])
            
            logging.info("✅ StageOneAndTwoCheck connected to MongoDB")
            return True
        except Exception as e:
            logging.error(f"❌ StageOneAndTwoCheck MongoDB connection failed: {e}")
            return False
    
    async def process_stage_one_results(self) -> Tuple[List[str], List[str], List[str]]:
        """
        Process Stage 1 results and separate masterplan, basiccommunity vs regular communities.
        
        Output: Tuple[regular_listing_ids, masterplan_listing_ids, basiccommunity_listing_ids]
        Description: Analyzes homepagedata for special communities and routes appropriately
        """
        try:
            logging.info("🔍 Analyzing Stage 1 results for special community types...")
            
            # Get all active listings from homepagedata
            cursor = self.homepagedata_collection.find(
                {"listing_status": {"$in": ["new", "updated", "active"]}},
                {"listing_id": 1, "property_data": 1, "scraped_at": 1, "source_url": 1, "data_source": 1, "listing_status": 1}
            )
            
            regular_listings = []
            masterplan_listings = []
            basiccommunity_listings = []
            masterplan_processed = 0
            basiccommunity_processed = 0
            
            async for doc in cursor:
                listing_id = doc.get("listing_id", "")
                
                if self._is_masterplan_community(listing_id):
                    # Process masterplan community
                    await self._process_masterplan_community(doc)
                    masterplan_listings.append(listing_id)
                    masterplan_processed += 1
                elif self._is_basiccommunity_community(listing_id):
                    # Process basic community
                    await self._process_basiccommunity_community(doc)
                    basiccommunity_listings.append(listing_id)
                    basiccommunity_processed += 1
                else:
                    # Regular community - continue to Stage 2
                    regular_listings.append(listing_id)
            
            logging.info(f"📊 Stage 1 Analysis Complete:")
            logging.info(f"   🏘️ Regular communities (to Stage 2): {len(regular_listings)}")
            logging.info(f"   🏗️ Masterplan communities (to masterplandata): {len(masterplan_listings)}")
            logging.info(f"   🏠 Basic communities (to basiccommunitydata): {len(basiccommunity_listings)}")
            logging.info(f"   ✅ Masterplan documents processed: {masterplan_processed}")
            logging.info(f"   ✅ Basic community documents processed: {basiccommunity_processed}")
            
            return regular_listings, masterplan_listings, basiccommunity_listings
            
        except Exception as e:
            logging.error(f"❌ Error processing Stage 1 results: {e}")
            return [], [], []
    
    def _is_masterplan_community(self, listing_id: str) -> bool:
        """
        Check if listing_id contains 'masterplan'.
        
        Input: listing_id (str)
        Output: bool - True if masterplan community
        """
        return "masterplan" in listing_id.lower()
    
    def _is_basiccommunity_community(self, listing_id: str) -> bool:
        """
        Check if listing_id contains 'basiccommunity'.
        
        Input: listing_id (str)
        Output: bool - True if basic community
        """
        return "basiccommunity" in listing_id.lower()
    
    async def _process_masterplan_community(self, homepage_doc: Dict):
        """
        Process masterplan community and save to masterplandata collection.
        
        Input: homepage_doc (Dict) - document from homepagedata
        """
        try:
            listing_id = homepage_doc.get("listing_id")
            
            # Transform document structure for masterplandata
            masterplan_doc = {
                "masterplanlisting_id": listing_id,  # Change from listing_id
                "scraped_at": homepage_doc.get("scraped_at"),
                "source_url": homepage_doc.get("source_url"),
                "masterplan_data": self._transform_property_data(homepage_doc.get("property_data", {})),  # Change from property_data
                "data_source": homepage_doc.get("data_source"),
                "listing_status": homepage_doc.get("listing_status"),
                "last_updated": datetime.now()
            }
            
            # Check if masterplan already exists
            existing_doc = await self.masterplandata_collection.find_one(
                {"masterplanlisting_id": listing_id}
            )
            
            if existing_doc:
                # Update existing masterplan
                masterplan_doc["previous_scraped_at"] = existing_doc.get("scraped_at")
                
                await self.masterplandata_collection.replace_one(
                    {"masterplanlisting_id": listing_id},
                    masterplan_doc
                )
                logging.info(f"🔄 Updated masterplan: {listing_id}")
            else:
                # Insert new masterplan
                await self.masterplandata_collection.insert_one(masterplan_doc)
                logging.info(f"🆕 New masterplan: {listing_id}")
            
        except Exception as e:
            logging.error(f"❌ Error processing masterplan community {listing_id}: {e}")
    
    async def _process_basiccommunity_community(self, homepage_doc: Dict):
        """
        Process basic community and save to basiccommunitydata collection.
        
        Input: homepage_doc (Dict) - document from homepagedata
        """
        try:
            listing_id = homepage_doc.get("listing_id")
            
            # Transform document structure for basiccommunitydata
            basiccommunity_doc = {
                "basic_community_listing_id": listing_id,  # Change from listing_id
                "scraped_at": homepage_doc.get("scraped_at"),
                "source_url": homepage_doc.get("source_url"),
                "basic_community_data": homepage_doc.get("property_data", {}),  # Change from property_data
                "data_source": homepage_doc.get("data_source"),
                "listing_status": homepage_doc.get("listing_status"),
                "last_updated": datetime.now()
            }
            
            # Check if basic community already exists
            existing_doc = await self.basiccommunitydata_collection.find_one(
                {"basic_community_listing_id": listing_id}
            )
            
            if existing_doc:
                # Update existing basic community
                basiccommunity_doc["previous_scraped_at"] = existing_doc.get("scraped_at")
                
                await self.basiccommunitydata_collection.replace_one(
                    {"basic_community_listing_id": listing_id},
                    basiccommunity_doc
                )
                logging.info(f"🔄 Updated basic community: {listing_id}")
            else:
                # Insert new basic community
                await self.basiccommunitydata_collection.insert_one(basiccommunity_doc)
                logging.info(f"🆕 New basic community: {listing_id}")
            
        except Exception as e:
            logging.error(f"❌ Error processing basic community {listing_id}: {e}")
    
    def _transform_property_data(self, property_data: Dict) -> Dict:
        """
        Transform property_data to masterplan_data format.
        
        Input: property_data (Dict)
        Output: masterplan_data (Dict) with renamed fields
        """
        try:
            masterplan_data = property_data.copy()
            
            # Transform price to price_range if it exists
            if "price" in masterplan_data:
                masterplan_data["price_range"] = masterplan_data.pop("price")
            
            # Ensure price_range field exists (from offers.price if available)
            if "price_range" not in masterplan_data and "offers" in masterplan_data:
                offers = masterplan_data.get("offers", {})
                if "price" in offers:
                    masterplan_data["price_range"] = offers["price"]
            
            return masterplan_data
            
        except Exception as e:
            logging.error(f"❌ Error transforming property data: {e}")
            return property_data
    
    async def get_regular_communities_for_stage2(self) -> Dict:
        """
        Get regular communities for Stage 2 processing (excludes masterplan and basiccommunity).
        
        Output: Dict mapping _id to property data for Stage 2
        """
        try:
            property_data = {}
            
            # Get only regular communities (exclude masterplan and basiccommunity)
            cursor = self.homepagedata_collection.find(
                {
                    "listing_status": {"$in": ["new", "updated", "active"]},
                    "listing_id": {
                        "$not": {"$regex": "masterplan|basiccommunity", "$options": "i"}
                    }
                },
                {
                    "_id": 1, 
                    "listing_id": 1, 
                    "property_data.url": 1,
                    "property_data.address.county": 1,
                    "property_data.address.addressLocality": 1,
                    "property_data.address.postalCode": 1,
                    "property_data.Address.county": 1,
                    "property_data.Address.addressLocality": 1,
                    "property_data.Address.postalCode": 1,
                    "property_data.offers.offeredBy": 1,
                    "property_data.accommodationCategory": 1
                }
            )
            
            async for doc in cursor:
                if "_id" in doc and "property_data" in doc and "url" in doc["property_data"]:
                    # Handle both "Address" (capital) and "address" (lowercase) field names
                    property_data_obj = doc.get("property_data", {})
                    address_data = property_data_obj.get("Address") or property_data_obj.get("address", {})
                    
                    property_data[doc["_id"]] = {
                        "url": property_data_obj["url"],
                        "listing_id": doc.get("listing_id"),
                        "county": address_data.get("county"),
                        "addressLocality": address_data.get("addressLocality"),
                        "postalCode": address_data.get("postalCode"),
                        "offeredBy": property_data_obj.get("offers", {}).get("offeredBy"),
                        "accommodationCategory": property_data_obj.get("accommodationCategory")
                    }
            
            logging.info(f"✅ Found {len(property_data)} regular communities for Stage 2")
            return property_data
            
        except Exception as e:
            logging.error(f"❌ Error getting regular communities for Stage 2: {e}")
            return {}
    
    async def handle_missing_stage1_listings(self):
        """Handle listings that are missing from current Stage 1 scrape"""
        try:
            # Get current active listings from today's scrape
            today = datetime.now().date()
            today_start = datetime.combine(today, datetime.min.time())
            tomorrow_start = today_start + timedelta(days=1)
            
            current_listings = set()
            async for doc in self.homepagedata_collection.find({
                "scraped_at": {"$gte": today_start.isoformat(), "$lt": tomorrow_start.isoformat()}
            }, {"listing_id": 1}):
                current_listings.add(doc.get("listing_id"))
            
            # Get all previously active listings
            previous_listings = set()
            async for doc in self.homepagedata_collection.find({
                "listing_status": {"$in": ["active", "new", "updated"]},
                "scraped_at": {"$lt": today_start.isoformat()}
            }, {"listing_id": 1}):
                previous_listings.add(doc.get("listing_id"))
            
            # Find missing listings
            missing_listings = previous_listings - current_listings
            
            if not missing_listings:
                logging.info("✅ No missing Stage 1 listings detected")
                return
            
            logging.info(f"📦 Archiving {len(missing_listings)} missing Stage 1 listings")
            
            # Move missing listings to archive
            for listing_id in missing_listings:
                doc = await self.homepagedata_collection.find_one({"listing_id": listing_id})
                if doc:
                    # Add archive metadata
                    doc["listing_status"] = "archived"
                    doc["archived_at"] = datetime.now()
                    doc["archive_reason"] = "missing from current Stage 1 scrape"
                    
                    # Insert to archive collection
                    await self.homepagedata_archived_collection.insert_one(doc)
                    
                    # Remove from active collection
                    await self.homepagedata_collection.delete_one({"listing_id": listing_id})
                    
                    logging.info(f"📦 Moved {listing_id} to homepagedata_archived")
            
            logging.info(f"✅ Successfully archived {len(missing_listings)} Stage 1 listings")
            
        except Exception as e:
            logging.error(f"❌ Error handling missing Stage 1 listings: {e}")
    
    def close_connection(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logging.info("🔌 StageOneAndTwoCheck MongoDB connection closed")


async def process_stage_one_to_two_routing():
    """
    Main function to handle routing between Stage 1 and Stage 2.
    
    Returns: property_data dict for Stage 2 processing (excludes masterplans)
    """
    checker = StageOneAndTwoCheck()
    
    try:
        # Connect to MongoDB
        if not await checker.connect_to_mongodb():
            logging.error("❌ Failed to connect to MongoDB")
            return {}
        
        # Handle missing Stage 1 listings first
        await checker.handle_missing_stage1_listings()
        
        # Process Stage 1 results and separate special vs regular communities
        regular_listings, masterplan_listings, basiccommunity_listings = await checker.process_stage_one_results()
        
        # Get regular community data for Stage 2
        stage2_property_data = await checker.get_regular_communities_for_stage2()
        
        logging.info(f"📋 Routing Summary:")
        logging.info(f"   ➡️ Stage 2 Properties: {len(stage2_property_data)}")
        logging.info(f"   🏗️ Masterplan Properties: {len(masterplan_listings)}")
        logging.info(f"   🏠 Basic Community Properties: {len(basiccommunity_listings)}")
        
        return stage2_property_data
        
    except Exception as e:
        logging.error(f"❌ Error in stage one to two routing: {e}")
        return {}
    finally:
        checker.close_connection()


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Test the routing logic
    result = asyncio.run(process_stage_one_to_two_routing())
    print(f"Processed {len(result)} properties for Stage 2")

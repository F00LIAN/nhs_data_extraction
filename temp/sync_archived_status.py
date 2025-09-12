#!/usr/bin/env python3
"""
Sync Archived Status Script
Compares communitydata_archived (archived db) with price_history_permanent (newhomesource db)
and updates listing_status to "archived" for matching communities

Purpose: Retroactively apply archived status to price history records
"""

import asyncio
import os
import logging
import hashlib
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

class ArchivedStatusSyncer:
    def __init__(self):
        self.client = None
        self.newhomesource_db = None
        self.archived_db = None
        
    async def connect(self):
        """Connect to MongoDB databases"""
        try:
            uri = os.getenv("MONGO_DB_URI")
            self.client = AsyncIOMotorClient(uri)
            self.newhomesource_db = self.client['newhomesource']
            self.archived_db = self.client['archived']
            
            await self.client.admin.command('ping')
            logging.info("✅ Connected to MongoDB")
            return True
        except Exception as e:
            logging.error(f"❌ MongoDB connection failed: {e}")
            return False
    
    def generate_permanent_id(self, community_id: str) -> str:
        """Generate consistent permanent ID from community_id"""
        return hashlib.md5(community_id.encode()).hexdigest()
    
    async def get_archived_community_ids(self):
        """Get all community_ids from archived communitydata"""
        try:
            archived_community_ids = set()
            
            # Get all documents from communitydata_archived in archived db
            archived_docs = await self.archived_db['communitydata_archived'].find({}).to_list(length=None)
            
            logging.info(f"📊 Found {len(archived_docs)} archived community documents")
            
            for doc in archived_docs:
                communities = doc.get("community_data", {}).get("communities", [])
                for community in communities:
                    community_id = community.get("community_id")
                    if community_id:
                        archived_community_ids.add(community_id)
            
            logging.info(f"📊 Extracted {len(archived_community_ids)} unique archived community IDs")
            return archived_community_ids
            
        except Exception as e:
            logging.error(f"❌ Error getting archived community IDs: {e}")
            return set()
    
    async def update_price_history_status(self, archived_community_ids):
        """Update listing_status in price_history_permanent for archived communities"""
        try:
            price_history_collection = self.newhomesource_db['price_history_permanent']
            
            updated_count = 0
            already_archived_count = 0
            not_found_count = 0
            
            logging.info(f"🔄 Processing {len(archived_community_ids)} archived community IDs...")
            
            for community_id in archived_community_ids:
                permanent_id = self.generate_permanent_id(community_id)
                
                # Check if record exists in price_history_permanent
                existing_record = await price_history_collection.find_one(
                    {"permanent_property_id": permanent_id}
                )
                
                if not existing_record:
                    not_found_count += 1
                    logging.debug(f"⚠️ No price history found for {community_id}")
                    continue
                
                # Check current status
                current_status = existing_record.get("listing_status", "")
                
                if current_status == "archived":
                    already_archived_count += 1
                    logging.debug(f"✅ Already archived: {community_id}")
                    continue
                
                # Update to archived status
                result = await price_history_collection.update_one(
                    {"permanent_property_id": permanent_id},
                    {"$set": {
                        "listing_status": "archived",
                        "archived_at": datetime.now(),
                        "last_updated": datetime.now(),
                        "archive_sync_reason": "retroactive_sync_from_communitydata_archived"
                    }}
                )
                
                if result.modified_count > 0:
                    updated_count += 1
                    logging.info(f"✅ Updated {community_id} to archived status")
                
                # Progress logging
                if (updated_count + already_archived_count + not_found_count) % 50 == 0:
                    total_processed = updated_count + already_archived_count + not_found_count
                    logging.info(f"📊 Progress: {total_processed}/{len(archived_community_ids)} processed")
            
            # Final summary
            logging.info(f"✅ Sync completed:")
            logging.info(f"   📦 Updated to archived: {updated_count}")
            logging.info(f"   ✅ Already archived: {already_archived_count}")
            logging.info(f"   ⚠️ Not found in price history: {not_found_count}")
            logging.info(f"   📊 Total processed: {updated_count + already_archived_count + not_found_count}")
            
            return {
                "updated": updated_count,
                "already_archived": already_archived_count,
                "not_found": not_found_count,
                "total_processed": updated_count + already_archived_count + not_found_count
            }
            
        except Exception as e:
            logging.error(f"❌ Error updating price history status: {e}")
            return None
    
    async def validate_sync(self):
        """Validate that archived communities are properly marked in price history"""
        try:
            # Count archived communities in price_history_permanent
            price_history_collection = self.newhomesource_db['price_history_permanent']
            archived_price_count = await price_history_collection.count_documents({
                "listing_status": "archived"
            })
            
            # Count total archived community IDs
            archived_community_ids = await self.get_archived_community_ids()
            
            logging.info(f"📊 Validation results:")
            logging.info(f"   🏠 Archived communities in source: {len(archived_community_ids)}")
            logging.info(f"   💰 Archived price histories: {archived_price_count}")
            
            return {
                "archived_communities": len(archived_community_ids),
                "archived_price_histories": archived_price_count
            }
            
        except Exception as e:
            logging.error(f"❌ Error during validation: {e}")
            return None
    
    async def run_sync(self):
        """Execute the archived status sync"""
        if not await self.connect():
            return False
        
        logging.info("🚀 Starting archived status sync...")
        
        # Get archived community IDs
        archived_community_ids = await self.get_archived_community_ids()
        
        if not archived_community_ids:
            logging.warning("⚠️ No archived community IDs found")
            return False
        
        # Update price history status
        sync_results = await self.update_price_history_status(archived_community_ids)
        
        if sync_results:
            logging.info("✅ Sync completed successfully")
            
            # Validate results
            await self.validate_sync()
        else:
            logging.error("❌ Sync failed")
            return False
        
        self.client.close()
        return True

def setup_logging():
    """Setup logging configuration"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"archived_status_sync_log_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    return log_filename

async def main():
    """Main execution function"""
    log_file = setup_logging()
    logging.info("🔄 Archived Status Sync Starting...")
    
    syncer = ArchivedStatusSyncer()
    success = await syncer.run_sync()
    
    if success:
        logging.info("🎉 Sync completed successfully")
        print(f"📋 Sync log: {log_file}")
    else:
        logging.error("❌ Sync failed")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)

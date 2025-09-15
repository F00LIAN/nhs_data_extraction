#!/usr/bin/env python3
"""
Basic Community Migration Script
Migrates existing basiccommunity listings from communitydata to basiccommunitydata collection
"""

import os
import sys
import asyncio
import logging
from datetime import datetime
from typing import Dict, List
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

# Add the scraper directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()


class BasicCommunityMigrator:
    """Migrates basic community listings from communitydata to basiccommunitydata"""
    
    def __init__(self):
        self.client = None
        self.db = None
        self.communitydata_collection = None
        self.basiccommunitydata_collection = None
        self.homepagedata_collection = None
        
        # Migration statistics
        self.stats = {
            "total_checked": 0,
            "basiccommunities_found": 0,
            "basiccommunities_migrated": 0,
            "basiccommunities_removed": 0,
            "errors": 0,
            "start_time": datetime.now()
        }
        
        # Setup logging
        self.setup_logging()
    
    def setup_logging(self):
        """Setup logging for migration operations"""
        log_dir = os.path.join(os.path.dirname(__file__), "logs")
        os.makedirs(log_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"basiccommunity_migration_log_{timestamp}.log")
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"📋 Basic community migration started - Log: {log_file}")
    
    async def connect_to_mongodb(self) -> bool:
        """Connect to MongoDB collections"""
        try:
            uri = os.getenv("MONGO_DB_URI")
            if not uri:
                self.logger.error("❌ MONGO_DB_URI environment variable not found")
                return False
            
            self.client = AsyncIOMotorClient(uri)
            self.db = self.client['newhomesource']
            
            self.communitydata_collection = self.db['communitydata']
            self.basiccommunitydata_collection = self.db['basiccommunitydata']
            self.homepagedata_collection = self.db['homepagedata']
            
            # Test connection
            await self.client.admin.command('ping')
            
            # Create indexes for basiccommunitydata collection
            await self.basiccommunitydata_collection.create_index([("basic_community_listing_id", 1)])
            
            self.logger.info("✅ Connected to MongoDB successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ MongoDB connection failed: {e}")
            return False
    
    async def find_basiccommunity_listings(self) -> List[Dict]:
        """Find all basic community listings in communitydata collection"""
        try:
            self.logger.info("🔍 Scanning communitydata collection for basic community listings...")
            
            basiccommunity_docs = []
            total_docs = 0
            
            # Query all documents in communitydata
            cursor = self.communitydata_collection.find({})
            
            async for doc in cursor:
                total_docs += 1
                listing_id = doc.get("listing_id", "")
                
                # Check if listing_id contains "basiccommunity"
                if self._is_basiccommunity_listing(listing_id):
                    basiccommunity_docs.append(doc)
                    self.logger.info(f"🏠 Found basic community: {listing_id}")
            
            self.stats["total_checked"] = total_docs
            self.stats["basiccommunities_found"] = len(basiccommunity_docs)
            
            self.logger.info(f"📊 Scan complete: {len(basiccommunity_docs)} basic communities found in {total_docs} total documents")
            return basiccommunity_docs
            
        except Exception as e:
            self.logger.error(f"❌ Error finding basic community listings: {e}")
            self.stats["errors"] += 1
            return []
    
    def _is_basiccommunity_listing(self, listing_id: str) -> bool:
        """Check if listing_id contains 'basiccommunity'"""
        return "basiccommunity" in listing_id.lower()
    
    async def get_homepage_data(self, listing_id: str) -> Dict:
        """Get corresponding homepage data for basic community listing"""
        try:
            homepage_doc = await self.homepagedata_collection.find_one({"listing_id": listing_id})
            return homepage_doc if homepage_doc else {}
        except Exception as e:
            self.logger.error(f"❌ Error getting homepage data for {listing_id}: {e}")
            return {}
    
    def transform_community_to_basiccommunity(self, community_doc: Dict, homepage_doc: Dict) -> Dict:
        """Transform communitydata document to basiccommunitydata format"""
        try:
            listing_id = community_doc.get("listing_id")
            
            # Start with basic structure
            basiccommunity_doc = {
                "basic_community_listing_id": listing_id,  # Rename from listing_id
                "scraped_at": community_doc.get("scraped_at"),
                "listing_status": community_doc.get("listing_status", "active"),
                "last_updated": datetime.now()
            }
            
            # Get source_url and data from homepage data if available
            if homepage_doc:
                basiccommunity_doc["source_url"] = homepage_doc.get("source_url", "")
                basiccommunity_doc["data_source"] = homepage_doc.get("data_source", "unknown")
                basiccommunity_doc["basic_community_data"] = homepage_doc.get("property_data", {})
            else:
                # Fallback if no homepage data found
                self.logger.warning(f"⚠️ No homepage data found for {listing_id}, creating minimal basic_community_data")
                basiccommunity_doc["source_url"] = ""
                basiccommunity_doc["data_source"] = "migration_fallback"
                basiccommunity_doc["basic_community_data"] = {
                    "name": "Unknown Basic Community",
                    "url": listing_id
                }
            
            # Add migration metadata
            basiccommunity_doc["migration_info"] = {
                "migrated_from": "communitydata",
                "migration_date": datetime.now(),
                "original_community_data_preserved": True if community_doc.get("community_data") else False
            }
            
            # Preserve previous timestamps if they exist
            if community_doc.get("previous_scraped_at"):
                basiccommunity_doc["previous_scraped_at"] = community_doc.get("previous_scraped_at")
            
            return basiccommunity_doc
            
        except Exception as e:
            self.logger.error(f"❌ Error transforming basic community document: {e}")
            return {}
    
    async def migrate_basiccommunity_listing(self, community_doc: Dict) -> bool:
        """Migrate a single basic community listing from communitydata to basiccommunitydata"""
        try:
            listing_id = community_doc.get("listing_id")
            self.logger.info(f"🔄 Migrating basic community: {listing_id}")
            
            # Get corresponding homepage data
            homepage_doc = await self.get_homepage_data(listing_id)
            
            # Transform to basic community format
            basiccommunity_doc = self.transform_community_to_basiccommunity(community_doc, homepage_doc)
            
            if not basiccommunity_doc:
                self.logger.error(f"❌ Failed to transform basic community document for {listing_id}")
                return False
            
            # Insert into basiccommunitydata collection
            result = await self.basiccommunitydata_collection.update_one(
                {"basic_community_listing_id": listing_id},
                {"$set": basiccommunity_doc},
                upsert=True
            )
            
            if result.upserted_id or result.modified_count > 0:
                self.logger.info(f"✅ Basic community inserted/updated in basiccommunitydata: {listing_id}")
                self.stats["basiccommunities_migrated"] += 1
                return True
            else:
                self.logger.error(f"❌ Failed to insert basic community into basiccommunitydata: {listing_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error migrating basic community {listing_id}: {e}")
            self.stats["errors"] += 1
            return False
    
    async def remove_from_communitydata(self, listing_id: str) -> bool:
        """Remove basic community listing from communitydata collection"""
        try:
            result = await self.communitydata_collection.delete_one({"listing_id": listing_id})
            
            if result.deleted_count > 0:
                self.logger.info(f"🗑️ Removed basic community from communitydata: {listing_id}")
                self.stats["basiccommunities_removed"] += 1
                return True
            else:
                self.logger.warning(f"⚠️ No document found to remove from communitydata: {listing_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error removing basic community from communitydata {listing_id}: {e}")
            self.stats["errors"] += 1
            return False
    
    async def execute_migration(self) -> bool:
        """Execute the complete migration process"""
        try:
            self.logger.info("🚀 Starting basic community migration from communitydata to basiccommunitydata")
            
            # Connect to MongoDB
            if not await self.connect_to_mongodb():
                return False
            
            # Find all basic community listings in communitydata
            basiccommunity_docs = await self.find_basiccommunity_listings()
            
            if not basiccommunity_docs:
                self.logger.info("ℹ️ No basic community listings found in communitydata collection")
                return True
            
            # Migrate each basic community listing
            successful_migrations = []
            failed_migrations = []
            
            for doc in basiccommunity_docs:
                listing_id = doc.get("listing_id")
                
                # Migrate to basiccommunitydata
                migration_success = await self.migrate_basiccommunity_listing(doc)
                
                if migration_success:
                    # Remove from communitydata
                    removal_success = await self.remove_from_communitydata(listing_id)
                    
                    if removal_success:
                        successful_migrations.append(listing_id)
                    else:
                        self.logger.error(f"❌ Migration succeeded but removal failed for: {listing_id}")
                        failed_migrations.append(listing_id)
                else:
                    failed_migrations.append(listing_id)
            
            # Final summary
            self.log_migration_summary(successful_migrations, failed_migrations)
            
            return len(failed_migrations) == 0
            
        except Exception as e:
            self.logger.error(f"❌ Migration execution failed: {e}")
            return False
        finally:
            if self.client:
                self.client.close()
                self.logger.info("🔌 MongoDB connection closed")
    
    def log_migration_summary(self, successful: List[str], failed: List[str]):
        """Log comprehensive migration summary"""
        duration = datetime.now() - self.stats["start_time"]
        
        self.logger.info("📊 BASIC COMMUNITY MIGRATION SUMMARY")
        self.logger.info("=" * 50)
        self.logger.info(f"⏱️ Duration: {duration}")
        self.logger.info(f"📋 Total documents checked: {self.stats['total_checked']}")
        self.logger.info(f"🏠 Basic communities found: {self.stats['basiccommunities_found']}")
        self.logger.info(f"✅ Successfully migrated: {self.stats['basiccommunities_migrated']}")
        self.logger.info(f"🗑️ Removed from communitydata: {self.stats['basiccommunities_removed']}")
        self.logger.info(f"❌ Errors encountered: {self.stats['errors']}")
        
        if successful:
            self.logger.info(f"✅ Successfully migrated listings:")
            for listing_id in successful:
                self.logger.info(f"   - {listing_id}")
        
        if failed:
            self.logger.error(f"❌ Failed migrations:")
            for listing_id in failed:
                self.logger.error(f"   - {listing_id}")
        
        if not failed:
            self.logger.info("🎉 All basic community migrations completed successfully!")
        else:
            self.logger.warning(f"⚠️ {len(failed)} migrations failed - manual review required")


async def main():
    """Main execution function"""
    print("🏠 Basic Community Migration Tool")
    print("Migrating basic community listings from communitydata to basiccommunitydata")
    print("=" * 70)
    
    migrator = BasicCommunityMigrator()
    success = await migrator.execute_migration()
    
    if success:
        print("\n✅ Migration completed successfully!")
        return 0
    else:
        print("\n❌ Migration completed with errors - check logs for details")
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n🛑 Migration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Fatal error: {e}")
        sys.exit(1)

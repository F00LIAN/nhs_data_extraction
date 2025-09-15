#!/usr/bin/env python3
"""
Masterplan Migration Script
Migrates existing masterplan listings from communitydata to masterplandata collection

This script:
1. Identifies masterplan listings in communitydata collection
2. Transforms the data structure to match masterplandata schema
3. Moves masterplan listings to masterplandata collection
4. Removes masterplan listings from communitydata collection
5. Logs all operations for audit trail
"""

import os
import sys
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Tuple
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

# Add the scraper directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()


class MasterplanMigrator:
    """Migrates masterplan listings from communitydata to masterplandata"""
    
    def __init__(self):
        self.client = None
        self.db = None
        self.communitydata_collection = None
        self.masterplandata_collection = None
        self.homepagedata_collection = None
        
        # Migration statistics
        self.stats = {
            "total_checked": 0,
            "masterplans_found": 0,
            "masterplans_migrated": 0,
            "masterplans_removed": 0,
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
        log_file = os.path.join(log_dir, f"masterplan_migration_log_{timestamp}.log")
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"📋 Masterplan migration started - Log: {log_file}")
    
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
            self.masterplandata_collection = self.db['masterplandata']
            self.homepagedata_collection = self.db['homepagedata']
            
            # Test connection
            await self.client.admin.command('ping')
            
            # Create indexes for masterplandata collection
            await self.masterplandata_collection.create_index([("masterplanlisting_id", 1)])
            
            self.logger.info("✅ Connected to MongoDB successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ MongoDB connection failed: {e}")
            return False
    
    async def find_masterplan_listings(self) -> List[Dict]:
        """
        Find all masterplan listings in communitydata collection
        
        Returns: List of community documents that contain masterplan listings
        """
        try:
            self.logger.info("🔍 Scanning communitydata collection for masterplan listings...")
            
            masterplan_docs = []
            total_docs = 0
            
            # Query all documents in communitydata
            cursor = self.communitydata_collection.find({})
            
            async for doc in cursor:
                total_docs += 1
                listing_id = doc.get("listing_id", "")
                
                # Check if listing_id contains "masterplan"
                if self._is_masterplan_listing(listing_id):
                    masterplan_docs.append(doc)
                    self.logger.info(f"🏗️ Found masterplan: {listing_id}")
            
            self.stats["total_checked"] = total_docs
            self.stats["masterplans_found"] = len(masterplan_docs)
            
            self.logger.info(f"📊 Scan complete: {len(masterplan_docs)} masterplans found in {total_docs} total documents")
            return masterplan_docs
            
        except Exception as e:
            self.logger.error(f"❌ Error finding masterplan listings: {e}")
            self.stats["errors"] += 1
            return []
    
    def _is_masterplan_listing(self, listing_id: str) -> bool:
        """Check if listing_id contains 'masterplan'"""
        return "masterplan" in listing_id.lower()
    
    async def get_homepage_data(self, listing_id: str) -> Dict:
        """Get corresponding homepage data for masterplan listing"""
        try:
            homepage_doc = await self.homepagedata_collection.find_one({"listing_id": listing_id})
            return homepage_doc if homepage_doc else {}
        except Exception as e:
            self.logger.error(f"❌ Error getting homepage data for {listing_id}: {e}")
            return {}
    
    def transform_community_to_masterplan(self, community_doc: Dict, homepage_doc: Dict) -> Dict:
        """
        Transform communitydata document to masterplandata format
        
        Input: community_doc from communitydata, homepage_doc from homepagedata
        Output: transformed document for masterplandata
        """
        try:
            listing_id = community_doc.get("listing_id")
            
            # Start with basic structure
            masterplan_doc = {
                "masterplanlisting_id": listing_id,  # Rename from listing_id
                "scraped_at": community_doc.get("scraped_at"),
                "listing_status": community_doc.get("listing_status", "active"),
                "last_updated": datetime.now()
            }
            
            # Get source_url from homepage data if available
            if homepage_doc:
                masterplan_doc["source_url"] = homepage_doc.get("source_url", "")
                masterplan_doc["data_source"] = homepage_doc.get("data_source", "unknown")
                
                # Transform property_data to masterplan_data
                property_data = homepage_doc.get("property_data", {})
                masterplan_doc["masterplan_data"] = self._transform_property_data(property_data)
            else:
                # Fallback if no homepage data found
                self.logger.warning(f"⚠️ No homepage data found for {listing_id}, creating minimal masterplan_data")
                masterplan_doc["source_url"] = ""
                masterplan_doc["data_source"] = "migration_fallback"
                masterplan_doc["masterplan_data"] = {
                    "name": "Unknown Masterplan",
                    "url": listing_id,
                    "price_range": "Price not available"
                }
            
            # Add migration metadata
            masterplan_doc["migration_info"] = {
                "migrated_from": "communitydata",
                "migration_date": datetime.now(),
                "original_community_data_preserved": True if community_doc.get("community_data") else False
            }
            
            # Preserve previous timestamps if they exist
            if community_doc.get("previous_scraped_at"):
                masterplan_doc["previous_scraped_at"] = community_doc.get("previous_scraped_at")
            
            return masterplan_doc
            
        except Exception as e:
            self.logger.error(f"❌ Error transforming masterplan document: {e}")
            return {}
    
    def _transform_property_data(self, property_data: Dict) -> Dict:
        """Transform property_data to masterplan_data format (same logic as stage_one_and_two_check.py)"""
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
            self.logger.error(f"❌ Error transforming property data: {e}")
            return property_data
    
    async def migrate_masterplan_listing(self, community_doc: Dict) -> bool:
        """
        Migrate a single masterplan listing from communitydata to masterplandata
        
        Returns: True if successful, False otherwise
        """
        try:
            listing_id = community_doc.get("listing_id")
            self.logger.info(f"🔄 Migrating masterplan: {listing_id}")
            
            # Get corresponding homepage data
            homepage_doc = await self.get_homepage_data(listing_id)
            
            # Transform to masterplan format
            masterplan_doc = self.transform_community_to_masterplan(community_doc, homepage_doc)
            
            if not masterplan_doc:
                self.logger.error(f"❌ Failed to transform masterplan document for {listing_id}")
                return False
            
            # Insert into masterplandata collection
            result = await self.masterplandata_collection.update_one(
                {"masterplanlisting_id": listing_id},
                {"$set": masterplan_doc},
                upsert=True
            )
            
            if result.upserted_id or result.modified_count > 0:
                self.logger.info(f"✅ Masterplan inserted/updated in masterplandata: {listing_id}")
                self.stats["masterplans_migrated"] += 1
                return True
            else:
                self.logger.error(f"❌ Failed to insert masterplan into masterplandata: {listing_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error migrating masterplan {listing_id}: {e}")
            self.stats["errors"] += 1
            return False
    
    async def remove_from_communitydata(self, listing_id: str) -> bool:
        """Remove masterplan listing from communitydata collection"""
        try:
            result = await self.communitydata_collection.delete_one({"listing_id": listing_id})
            
            if result.deleted_count > 0:
                self.logger.info(f"🗑️ Removed masterplan from communitydata: {listing_id}")
                self.stats["masterplans_removed"] += 1
                return True
            else:
                self.logger.warning(f"⚠️ No document found to remove from communitydata: {listing_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error removing masterplan from communitydata {listing_id}: {e}")
            self.stats["errors"] += 1
            return False
    
    async def execute_migration(self) -> bool:
        """
        Execute the complete migration process
        
        Returns: True if migration completed successfully
        """
        try:
            self.logger.info("🚀 Starting masterplan migration from communitydata to masterplandata")
            
            # Connect to MongoDB
            if not await self.connect_to_mongodb():
                return False
            
            # Find all masterplan listings in communitydata
            masterplan_docs = await self.find_masterplan_listings()
            
            if not masterplan_docs:
                self.logger.info("ℹ️ No masterplan listings found in communitydata collection")
                return True
            
            # Migrate each masterplan listing
            successful_migrations = []
            failed_migrations = []
            
            for doc in masterplan_docs:
                listing_id = doc.get("listing_id")
                
                # Migrate to masterplandata
                migration_success = await self.migrate_masterplan_listing(doc)
                
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
        
        self.logger.info("📊 MASTERPLAN MIGRATION SUMMARY")
        self.logger.info("=" * 50)
        self.logger.info(f"⏱️ Duration: {duration}")
        self.logger.info(f"📋 Total documents checked: {self.stats['total_checked']}")
        self.logger.info(f"🏗️ Masterplans found: {self.stats['masterplans_found']}")
        self.logger.info(f"✅ Successfully migrated: {self.stats['masterplans_migrated']}")
        self.logger.info(f"🗑️ Removed from communitydata: {self.stats['masterplans_removed']}")
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
            self.logger.info("🎉 All masterplan migrations completed successfully!")
        else:
            self.logger.warning(f"⚠️ {len(failed)} migrations failed - manual review required")


async def main():
    """Main execution function"""
    print("🏗️ Masterplan Migration Tool")
    print("Migrating masterplan listings from communitydata to masterplandata")
    print("=" * 60)
    
    migrator = MasterplanMigrator()
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

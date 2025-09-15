#!/usr/bin/env python3
"""
Migration Script: Add offeredBy and accommodationCategory to communitydata
Purpose: Migrate existing communitydata documents to include offeredBy and accommodationCategory fields from homepagedata
Created: 2025-01-28
"""

import asyncio
import os
import logging
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'migration_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

class CommunityDataMigration:
    """Handles migration of offeredBy and accommodationCategory fields to communitydata"""
    
    def __init__(self):
        self.uri = os.getenv("MONGO_DB_URI")
        if not self.uri:
            raise ValueError("MONGO_DB_URI environment variable is required")
        self.client = None
        self.db = None
        self.homepagedata_collection = None
        self.communitydata_collection = None
        
        # Migration statistics
        self.stats = {
            "total_communitydata_docs": 0,
            "successfully_migrated": 0,
            "homepage_data_not_found": 0,
            "no_changes_needed": 0,
            "errors": 0,
            "communities_updated": 0
        }
    
    async def connect_to_mongodb(self):
        """Connect to MongoDB"""
        try:
            self.client = AsyncIOMotorClient(self.uri)
            self.db = self.client['newhomesource']
            self.homepagedata_collection = self.db['homepagedata']
            self.communitydata_collection = self.db['communitydata']
            
            logging.info("✅ Connected to MongoDB successfully")
        except Exception as e:
            logging.error(f"❌ Failed to connect to MongoDB: {e}")
            raise
    
    async def get_homepage_metadata(self, listing_id: str) -> dict:
        """Get offeredBy and accommodationCategory from homepagedata for a listing"""
        try:
            doc = await self.homepagedata_collection.find_one(
                {"listing_id": listing_id},
                {
                    "property_data.offers.offeredBy": 1,
                    "property_data.accommodationCategory": 1
                }
            )
            
            if doc and "property_data" in doc:
                property_data = doc["property_data"]
                return {
                    "offeredBy": property_data.get("offers", {}).get("offeredBy"),
                    "accommodationCategory": property_data.get("accommodationCategory")
                }
            
            return {"offeredBy": None, "accommodationCategory": None}
            
        except Exception as e:
            logging.error(f"❌ Error fetching homepage metadata for {listing_id}: {e}")
            return {"offeredBy": None, "accommodationCategory": None}
    
    async def migrate_community_document(self, community_doc: dict) -> bool:
        """Migrate a single community document"""
        try:
            listing_id = community_doc.get("listing_id")
            if not listing_id:
                logging.warning(f"⚠️ Community document missing listing_id: {community_doc.get('_id')}")
                return False
            
            # Get homepage metadata
            homepage_metadata = await self.get_homepage_metadata(listing_id)
            
            if not homepage_metadata["offeredBy"] and not homepage_metadata["accommodationCategory"]:
                logging.warning(f"⚠️ No metadata found in homepagedata for listing_id: {listing_id}")
                self.stats["homepage_data_not_found"] += 1
                return False
            
            # Check if any communities need updating
            communities = community_doc.get("community_data", {}).get("communities", [])
            if not communities:
                logging.warning(f"⚠️ No communities found in document for listing_id: {listing_id}")
                return False
            
            # Update each community
            updated_communities = []
            changes_made = False
            
            for community in communities:
                # Only add fields if they don't already exist and we have values
                community_updated = False
                
                if homepage_metadata["offeredBy"] and "offeredBy" not in community:
                    community["offeredBy"] = homepage_metadata["offeredBy"]
                    community_updated = True
                
                if homepage_metadata["accommodationCategory"] and "accommodationCategory" not in community:
                    community["accommodationCategory"] = homepage_metadata["accommodationCategory"]
                    community_updated = True
                
                if community_updated:
                    changes_made = True
                    self.stats["communities_updated"] += 1
                    logging.debug(f"✅ Updated community: {community.get('name', 'Unknown')} for listing {listing_id}")
                
                updated_communities.append(community)
            
            if not changes_made:
                logging.info(f"ℹ️ No changes needed for listing_id: {listing_id}")
                self.stats["no_changes_needed"] += 1
                return True
            
            # Update the document in the database
            result = await self.communitydata_collection.update_one(
                {"_id": community_doc["_id"]},
                {
                    "$set": {
                        "community_data.communities": updated_communities,
                        "last_migration_update": datetime.now(),
                        "migration_version": "add_offered_by_accommodation_category_v1"
                    }
                }
            )
            
            if result.modified_count == 1:
                logging.info(f"✅ Successfully migrated listing_id: {listing_id} ({len(updated_communities)} communities)")
                self.stats["successfully_migrated"] += 1
                return True
            else:
                logging.error(f"❌ Failed to update document for listing_id: {listing_id}")
                return False
                
        except Exception as e:
            logging.error(f"❌ Error migrating community document {community_doc.get('_id')}: {e}")
            self.stats["errors"] += 1
            return False
    
    async def run_migration(self, dry_run: bool = False, limit: int = None):
        """Run the complete migration process"""
        try:
            await self.connect_to_mongodb()
            
            logging.info(f"🚀 Starting migration {'(DRY RUN)' if dry_run else ''}")
            
            # Get all community documents
            query = {}
            if limit:
                logging.info(f"📊 Processing limited to {limit} documents")
            
            cursor = self.communitydata_collection.find(query)
            if limit:
                cursor = cursor.limit(limit)
            
            # Count total documents
            total_docs = await self.communitydata_collection.count_documents(query)
            self.stats["total_communitydata_docs"] = min(total_docs, limit) if limit else total_docs
            
            logging.info(f"📊 Found {self.stats['total_communitydata_docs']} community documents to process")
            
            # Process each document
            processed = 0
            async for doc in cursor:
                processed += 1
                
                if processed % 100 == 0:
                    logging.info(f"📊 Progress: {processed}/{self.stats['total_communitydata_docs']} documents")
                
                if not dry_run:
                    await self.migrate_community_document(doc)
                else:
                    # Dry run - just log what would be done
                    listing_id = doc.get("listing_id")
                    communities = doc.get("community_data", {}).get("communities", [])
                    logging.info(f"[DRY RUN] Would process listing_id: {listing_id} with {len(communities)} communities")
            
            # Log final statistics
            self._log_final_statistics(dry_run)
            
        except Exception as e:
            logging.error(f"❌ Migration failed: {e}")
            raise
        finally:
            if self.client:
                self.client.close()
                logging.info("🔌 Database connection closed")
    
    def _log_final_statistics(self, dry_run: bool):
        """Log final migration statistics"""
        logging.info("📊 MIGRATION COMPLETE")
        logging.info(f"   📄 Total documents processed: {self.stats['total_communitydata_docs']}")
        
        if not dry_run:
            logging.info(f"   ✅ Successfully migrated: {self.stats['successfully_migrated']}")
            logging.info(f"   🏠 Communities updated: {self.stats['communities_updated']}")
            logging.info(f"   ⚠️ Homepage data not found: {self.stats['homepage_data_not_found']}")
            logging.info(f"   ℹ️ No changes needed: {self.stats['no_changes_needed']}")
            logging.info(f"   ❌ Errors: {self.stats['errors']}")
        else:
            logging.info("   [DRY RUN] No actual changes were made")

async def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate offeredBy and accommodationCategory to communitydata')
    parser.add_argument('--dry-run', action='store_true', help='Run in dry-run mode (no actual changes)')
    parser.add_argument('--limit', type=int, help='Limit number of documents to process')
    
    args = parser.parse_args()
    
    migration = CommunityDataMigration()
    await migration.run_migration(dry_run=args.dry_run, limit=args.limit)

if __name__ == "__main__":
    asyncio.run(main())

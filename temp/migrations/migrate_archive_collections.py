#!/usr/bin/env python3
"""
Archive Collections Migration Script
Moves archived data from newhomesource db to archived db

Migration Plan:
- archivedlistings (newhomesource) → homepagedata_archived (archived)
- communitydata_archived (newhomesource) → communitydata_archived (archived)
- basiccommunitydata_archived (newhomesource) → basiccommunitydata_archived (archived)
- masterplandata_archived (newhomesource) → masterplandata_archived (archived)
"""

import asyncio
import os
import logging
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

class ArchiveMigrator:
    def __init__(self):
        self.client = None
        self.newhomesource_db = None
        self.archived_db = None
        
    async def connect(self):
        """Connect to MongoDB"""
        try:
            uri = os.getenv("MONGO_DB_URI")
            self.client = AsyncIOMotorClient(uri)
            self.newhomesource_db = self.client['newhomesource']
            self.archived_db = self.client['archived']
            
            # Test connection
            await self.client.admin.command('ping')
            logging.info("✅ Connected to MongoDB")
            return True
        except Exception as e:
            logging.error(f"❌ MongoDB connection failed: {e}")
            return False
    
    async def migrate_collection(self, source_collection_name: str, target_collection_name: str = None):
        """Migrate documents from source to target collection"""
        target_name = target_collection_name or source_collection_name
        
        try:
            source_collection = self.newhomesource_db[source_collection_name]
            target_collection = self.archived_db[target_name]
            
            # Count documents to migrate
            doc_count = await source_collection.count_documents({})
            if doc_count == 0:
                logging.info(f"ℹ️ No documents found in {source_collection_name}")
                return
            
            logging.info(f"🔄 Migrating {doc_count} documents from {source_collection_name} → {target_name}")
            
            # Get all documents
            docs = await source_collection.find({}).to_list(length=None)
            
            if docs:
                # Add migration metadata
                for doc in docs:
                    doc["migrated_at"] = datetime.now()
                    doc["migration_source"] = f"newhomesource.{source_collection_name}"
                
                # Insert to target
                await target_collection.insert_many(docs)
                logging.info(f"✅ Inserted {len(docs)} documents to archived.{target_name}")
                
                # Verify insertion
                target_count = await target_collection.count_documents({})
                if target_count >= len(docs):
                    # Delete from source
                    delete_result = await source_collection.delete_many({})
                    logging.info(f"🗑️ Deleted {delete_result.deleted_count} documents from {source_collection_name}")
                else:
                    logging.error(f"❌ Target count mismatch for {target_name}")
            
        except Exception as e:
            logging.error(f"❌ Error migrating {source_collection_name}: {e}")
    
    async def run_migration(self):
        """Execute all archive migrations"""
        if not await self.connect():
            return False
        
        migrations = [
            ("archivedlistings", "homepagedata_archived"),
            ("communitydata_archived", "communitydata_archived"),
            ("basiccommunitydata_archived", "basiccommunitydata_archived"),
            ("masterplandata_archived", "masterplandata_archived")
        ]
        
        logging.info("🚀 Starting archive collection migration...")
        
        for source, target in migrations:
            await self.migrate_collection(source, target)
        
        logging.info("✅ Archive migration completed")
        self.client.close()
        return True

def setup_logging():
    """Setup logging configuration"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"archive_migration_log_{timestamp}.log"
    
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
    logging.info("🏠 Archive Collections Migration Starting...")
    
    migrator = ArchiveMigrator()
    success = await migrator.run_migration()
    
    if success:
        logging.info("🎉 Migration completed successfully")
        print(f"📋 Migration log: {log_file}")
    else:
        logging.error("❌ Migration failed")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)

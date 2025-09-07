#!/usr/bin/env python3
"""
Simple Field Removal Script
Purpose: Remove a specified field from all documents in a MongoDB collection
"""

import asyncio
import os
import logging
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration variables - EDIT THESE
COLLECTION_NAME = "communitydata"  # Replace with your collection name
FIELD_TO_REMOVE = "is_archived"           # Replace with the field you want to remove

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'field_removal_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

class FieldRemover:
    """Handles removal of a specified field from a MongoDB collection"""
    
    def __init__(self, collection_name: str, field_name: str):
        self.collection_name = collection_name
        self.field_name = field_name
        self.uri = os.getenv("MONGO_DB_URI")
        if not self.uri:
            raise ValueError("MONGO_DB_URI environment variable is required")
        self.client = None
        self.db = None
        self.collection = None
        
        # Statistics
        self.stats = {
            "total_documents": 0,
            "documents_updated": 0,
            "documents_without_field": 0,
            "errors": 0
        }
    
    async def connect_to_mongodb(self):
        """Connect to MongoDB"""
        try:
            self.client = AsyncIOMotorClient(self.uri)
            self.db = self.client['newhomesource']
            self.collection = self.db[self.collection_name]
            
            logging.info("✅ Connected to MongoDB successfully")
        except Exception as e:
            logging.error(f"❌ Failed to connect to MongoDB: {e}")
            raise
    
    async def count_documents_with_field(self) -> int:
        """Count how many documents have the field"""
        try:
            count = await self.collection.count_documents({self.field_name: {"$exists": True}})
            return count
        except Exception as e:
            logging.error(f"❌ Error counting documents: {e}")
            return 0
    
    async def remove_field(self):
        """Remove the field from all documents in the collection"""
        try:
            await self.connect_to_mongodb()
            
            # Get total document count
            self.stats["total_documents"] = await self.collection.count_documents({})
            
            # Count documents that have the field
            docs_with_field = await self.count_documents_with_field()
            
            logging.info(f"🚀 Starting field removal process")
            logging.info(f"📊 Collection: {self.collection_name}")
            logging.info(f"📊 Field to remove: {self.field_name}")
            logging.info(f"📊 Total documents: {self.stats['total_documents']}")
            logging.info(f"📊 Documents with field: {docs_with_field}")
            
            if docs_with_field == 0:
                logging.info("ℹ️ No documents contain the specified field. Nothing to remove.")
                return
            
            # Remove the field from all documents
            result = await self.collection.update_many(
                {self.field_name: {"$exists": True}},
                {"$unset": {self.field_name: ""}}
            )
            
            self.stats["documents_updated"] = result.modified_count
            self.stats["documents_without_field"] = self.stats["total_documents"] - docs_with_field
            
            # Log results
            logging.info("📊 FIELD REMOVAL COMPLETE")
            logging.info(f"   ✅ Documents updated: {self.stats['documents_updated']}")
            logging.info(f"   ℹ️ Documents without field: {self.stats['documents_without_field']}")
            logging.info(f"   📄 Total documents: {self.stats['total_documents']}")
            
            if self.stats["documents_updated"] > 0:
                logging.info(f"✅ Successfully removed field '{self.field_name}' from {self.stats['documents_updated']} documents")
            else:
                logging.warning("⚠️ No documents were updated")
                
        except Exception as e:
            logging.error(f"❌ Field removal failed: {e}")
            self.stats["errors"] += 1
            raise
        finally:
            if self.client:
                self.client.close()
                logging.info("🔌 Database connection closed")

async def main():
    """Main execution function"""
    logging.info(f"Starting field removal for collection: {COLLECTION_NAME}")
    logging.info(f"Field to remove: {FIELD_TO_REMOVE}")
    
    # Validate configuration
    if COLLECTION_NAME == "your_collection_name" or FIELD_TO_REMOVE == "field_name":
        logging.error("❌ Please update the COLLECTION_NAME and FIELD_TO_REMOVE variables at the top of the script")
        return
    
    remover = FieldRemover(COLLECTION_NAME, FIELD_TO_REMOVE)
    await remover.remove_field()

if __name__ == "__main__":
    asyncio.run(main())

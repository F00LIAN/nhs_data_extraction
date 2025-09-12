#!/usr/bin/env python3
"""
Remove 9-12-2025 entries from price_history_permanent collection
"""

import asyncio
import os
import logging
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

async def remove_sept_12_entries():
    """Remove all price timeline entries for 2025-09-12"""
    try:
        uri = os.getenv("MONGO_DB_URI")
        client = AsyncIOMotorClient(uri)
        db = client['newhomesource']
        collection = db['price_history_permanent']
        
        logging.info("🗑️ Removing 2025-09-12 entries from price timelines...")
        
        # Get all documents
        documents = await collection.find({}).to_list(length=None)
        
        updated_count = 0
        total_removed = 0
        
        for doc in documents:
            timeline = doc.get("price_timeline", [])
            original_length = len(timeline)
            
            # Filter out 2025-09-12 entries
            filtered_timeline = []
            for entry in timeline:
                date = entry.get("date")
                if isinstance(date, str):
                    date_obj = datetime.fromisoformat(date.replace('Z', '+00:00'))
                else:
                    date_obj = date
                
                # Keep if not September 12, 2025
                if date_obj.date() != datetime(2025, 9, 12).date():
                    filtered_timeline.append(entry)
            
            # Update if timeline changed
            if len(filtered_timeline) != original_length:
                await collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"price_timeline": filtered_timeline}}
                )
                removed = original_length - len(filtered_timeline)
                total_removed += removed
                updated_count += 1
                logging.info(f"✅ {doc['permanent_property_id']}: Removed {removed} entries")
        
        logging.info(f"🎉 Completed: Updated {updated_count} documents, removed {total_removed} total entries")
        client.close()
        
    except Exception as e:
        logging.error(f"❌ Error: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    asyncio.run(remove_sept_12_entries())

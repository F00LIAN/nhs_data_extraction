#!/usr/bin/env python3
"""
Price History Backfill Script
Backfills price_history_permanent collection with historical daily data from 9/11/2025 to first existing entry
Creates initial city snapshot data

Requirements:
- Adds daily entries to price_timeline from 9/11/2025 to existing data start
- Creates price_city_snapshot collection data
"""

import asyncio
import os
import logging
import hashlib
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

class PriceHistoryBackfiller:
    def __init__(self):
        self.client = None
        self.db = None
        self.price_history_permanent_collection = None
        self.price_city_snapshot_collection = None
        self.backfill_start_date = datetime(2025, 9, 11)
        
    async def connect(self):
        """Connect to MongoDB"""
        try:
            uri = os.getenv("MONGO_DB_URI")
            self.client = AsyncIOMotorClient(uri)
            self.db = self.client['newhomesource']
            self.price_history_permanent_collection = self.db['price_history_permanent']
            self.price_city_snapshot_collection = self.db['price_city_snapshot']
            
            await self.client.admin.command('ping')
            logging.info("✅ Connected to MongoDB")
            return True
        except Exception as e:
            logging.error(f"❌ MongoDB connection failed: {e}")
            return False
    
    def generate_city_id(self, city: str, county: str) -> str:
        """Generate consistent city ID"""
        return hashlib.md5(f"{city}_{county}".encode()).hexdigest()
    
    async def backfill_community_timeline(self, document: dict):
        """Backfill daily price entries for a single community"""
        try:
            permanent_id = document["permanent_property_id"]
            price_timeline = document.get("price_timeline", [])
            
            if not price_timeline:
                logging.warning(f"⚠️ No price timeline found for {permanent_id}")
                return
            
            # Sort timeline by date to find earliest entry
            sorted_timeline = sorted(price_timeline, key=lambda x: x["date"] if isinstance(x["date"], datetime) else datetime.fromisoformat(x["date"].replace('Z', '+00:00')))
            
            # Get first (earliest) entry date and price
            first_entry = sorted_timeline[0]
            first_date = first_entry["date"]
            if isinstance(first_date, str):
                first_date = datetime.fromisoformat(first_date.replace('Z', '+00:00'))
            elif hasattr(first_date, 'replace'):
                first_date = first_date.replace(tzinfo=None)
            
            first_price = first_entry["price"]
            
            # Deduplicate existing timeline - keep only one entry per calendar day (latest time for that day)
            deduplicated_timeline = {}
            for entry in price_timeline:
                entry_date = entry["date"]
                if isinstance(entry_date, str):
                    entry_date = datetime.fromisoformat(entry_date.replace('Z', '+00:00'))
                elif hasattr(entry_date, 'replace'):
                    entry_date = entry_date.replace(tzinfo=None)
                
                calendar_date = entry_date.date()
                
                # Keep the latest entry for each calendar day
                if calendar_date not in deduplicated_timeline or entry_date > deduplicated_timeline[calendar_date]["date"]:
                    deduplicated_timeline[calendar_date] = {
                        "date": entry_date,
                        "price": entry["price"],
                        "currency": entry["currency"],
                        "source": entry["source"],
                        "change_type": entry["change_type"],
                        "context": entry["context"]
                    }
            
            # Get existing calendar dates after deduplication
            existing_dates = set(deduplicated_timeline.keys())
            
            # Generate missing daily entries from first_date back to backfill_start_date (9/11/2025)
            first_date_clean = first_date.replace(tzinfo=None)
            current_date = first_date_clean.date()
            backfill_target_date = self.backfill_start_date.date()
            new_entries = []
            
            # First, ensure September 11, 2025 entry with specific timestamp
            sept_11_date = datetime(2025, 9, 11, 15, 50, 24, 202000)  # 2025-09-11T15:50:24.202+00:00
            if sept_11_date.date() not in existing_dates:
                sept_11_entry = {
                    "date": sept_11_date,
                    "price": first_price,
                    "currency": "USD",
                    "source": "backfill",
                    "change_type": "stable",
                    "context": {
                        "build_status": first_entry.get("context", {}).get("build_status", []),
                        "build_type": first_entry.get("context", {}).get("build_type", ""),
                        "change_percentage": 0
                    }
                }
                new_entries.append(sept_11_entry)
            
            # Go backwards from first entry date to September 11, 2025
            while current_date >= backfill_target_date:
                # Only add if date doesn't already exist and it's not September 11 (already handled above)
                if current_date not in existing_dates and current_date != sept_11_date.date():
                    new_entry = {
                        "date": datetime.combine(current_date, first_date_clean.time()),
                        "price": first_price,
                        "currency": "USD",
                        "source": "backfill",
                        "change_type": "stable",
                        "context": {
                            "build_status": first_entry.get("context", {}).get("build_status", []),
                            "build_type": first_entry.get("context", {}).get("build_type", ""),
                            "change_percentage": 0
                        }
                    }
                    new_entries.append(new_entry)
                current_date -= timedelta(days=1)
            
            # Forward backfill: Fill missing dates from latest entry to today
            today = datetime.now().date()
            
            # Get the latest entry date from either existing or newly added entries
            all_entries = list(deduplicated_timeline.values()) + new_entries
            if all_entries:
                latest_entry = max(all_entries, key=lambda x: x["date"])
                latest_date = latest_entry["date"]
                if isinstance(latest_date, str):
                    latest_date = datetime.fromisoformat(latest_date.replace('Z', '+00:00'))
                latest_date = latest_date.replace(tzinfo=None)
                
                # Forward fill from day after latest entry to today
                current_forward_date = (latest_date.date() + timedelta(days=1))
                
                while current_forward_date <= today:
                    if current_forward_date not in existing_dates:
                        # Use the latest known price for forward backfill
                        forward_entry = {
                            "date": datetime.combine(current_forward_date, latest_date.time()),
                            "price": latest_entry["price"],
                            "currency": "USD",
                            "source": "forward_backfill",
                            "change_type": "stable",
                            "context": {
                                "build_status": latest_entry.get("context", {}).get("build_status", []),
                                "build_type": latest_entry.get("context", {}).get("build_type", ""),
                                "change_percentage": 0
                            }
                        }
                        new_entries.append(forward_entry)
                    current_forward_date += timedelta(days=1)
            
            # Rebuild timeline from deduplicated entries + new backfill entries
            final_timeline = list(deduplicated_timeline.values()) + new_entries
            
            # Sort final timeline by date
            final_timeline.sort(key=lambda x: x["date"])
            
            if new_entries or len(final_timeline) != len(price_timeline):
                # Update document with deduplicated and backfilled timeline
                await self.price_history_permanent_collection.update_one(
                    {"permanent_property_id": permanent_id},
                    {"$set": {"price_timeline": final_timeline}}
                )
                
                duplicates_removed = len(price_timeline) - len(deduplicated_timeline)
                backward_entries = len([e for e in new_entries if e.get("source") == "backfill"])
                forward_entries = len([e for e in new_entries if e.get("source") == "forward_backfill"])
                sept_11_entries = len([e for e in new_entries if e.get("date", datetime.min).date() == datetime(2025, 9, 11).date()])
                
                logging.info(f"✅ Processed {permanent_id}: Added {backward_entries} backward, {forward_entries} forward, {sept_11_entries} Sept-11 entries, removed {duplicates_removed} duplicates")
                return len(new_entries)
            
        except Exception as e:
            logging.error(f"❌ Error backfilling {document.get('permanent_property_id')}: {e}")
            return 0
    
    async def create_city_snapshot(self):
        """Create initial city price snapshot data"""
        try:
            logging.info("🏙️ Creating initial city price snapshots...")
            
            # Aggregate data by city
            pipeline = [
                {
                    "$match": {
                        "listing_status": "active"
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "city": "$address.addressLocality",
                            "county": "$address.county",
                            "postal_code": "$address.postalCode"
                        },
                        "properties": {
                            "$push": {
                                "accommodation_category": "$accommodationCategory",
                                "price": "$aggregated_metrics.most_recent_price"
                            }
                        },
                        "avg_price": {"$avg": "$aggregated_metrics.most_recent_price"},
                        "min_price": {"$min": "$aggregated_metrics.most_recent_price"},
                        "max_price": {"$max": "$aggregated_metrics.most_recent_price"},
                        "count": {"$sum": 1}
                    }
                }
            ]
            
            city_aggregates = await self.price_history_permanent_collection.aggregate(pipeline).to_list(length=None)
            
            for city_data in city_aggregates:
                city_info = city_data["_id"]
                city_id = self.generate_city_id(city_info['city'], city_info['county'])
                
                # Separate by property type
                sfr_properties = [p for p in city_data["properties"] if p["accommodation_category"] == "Single Family Residence"]
                condo_properties = [p for p in city_data["properties"] if p["accommodation_category"] == "Condominium"]
                
                # Calculate averages by type
                sfr_avg = sum(p["price"] for p in sfr_properties) / len(sfr_properties) if sfr_properties else 0
                condo_avg = sum(p["price"] for p in condo_properties) / len(condo_properties) if condo_properties else 0
                
                # Build city snapshot document
                city_snapshot = {
                    "city_id": city_id,
                    "addressLocality": city_info["city"],
                    "county": city_info["county"],
                    "postalCode": city_info["postal_code"],
                    "aggregated_metrics": {
                        "average_price": city_data["avg_price"],
                        "min_price": city_data["min_price"],
                        "max_price": city_data["max_price"],
                        "total_number_of_properties": city_data["count"],
                        "property_types": list(set(p["accommodation_category"] for p in city_data["properties"])),
                        "single_family_residence_moving_averages": self._calculate_moving_averages(sfr_avg),
                        "single_family_residence_percent_change_metrics": self._calculate_percent_changes(),
                        "condominium_moving_averages": self._calculate_moving_averages(condo_avg),
                        "condominium_percent_change_metrics": self._calculate_percent_changes(),
                        "all_moving_averages": self._calculate_moving_averages(city_data["avg_price"]),
                        "all_percent_change_metrics": self._calculate_percent_changes()
                    },
                    "last_snapshot_date": datetime.now(),
                    "created_at": datetime.now()
                }
                
                # Upsert city snapshot
                await self.price_city_snapshot_collection.update_one(
                    {"city_id": city_id},
                    {"$set": city_snapshot},
                    upsert=True
                )
                
            logging.info(f"✅ Created {len(city_aggregates)} city price snapshots")
            
        except Exception as e:
            logging.error(f"❌ Error creating city snapshots: {e}")
    
    def _calculate_moving_averages(self, current_price: float) -> dict:
        """Calculate moving averages (initial values)"""
        return {
            "1_day_average": current_price,
            "7_day_average": current_price,
            "30_day_average": current_price,
            "90_day_average": current_price,
            "180_day_average": current_price,
            "365_day_average": current_price
        }
    
    def _calculate_percent_changes(self) -> dict:
        """Calculate percent changes (initial zeros)"""
        return {
            "1_day_change": 0,
            "7_day_change": 0,
            "30_day_change": 0,
            "90_day_change": 0,
            "180_day_change": 0,
            "365_day_change": 0
        }
    
    async def run_backfill(self):
        """Execute price history backfill"""
        if not await self.connect():
            return False
        
        logging.info("🚀 Starting price history backfill...")
        
        # Get all price history documents
        documents = await self.price_history_permanent_collection.find({}).to_list(length=None)
        
        total_entries_added = 0
        processed_count = 0
        
        for doc in documents:
            entries_added = await self.backfill_community_timeline(doc)
            total_entries_added += entries_added or 0
            processed_count += 1
            
            if processed_count % 100 == 0:
                logging.info(f"📊 Processed {processed_count}/{len(documents)} documents")
        
        logging.info(f"✅ Backfill completed: {total_entries_added} entries added across {processed_count} communities (backward + forward backfill, also deduplicated existing entries)")
        
        # Create city snapshots
        await self.create_city_snapshot()
        
        self.client.close()
        return True

def setup_logging():
    """Setup logging configuration"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"price_backfill_log_{timestamp}.log"
    
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
    logging.info("💰 Price History Backfill Starting...")
    
    backfiller = PriceHistoryBackfiller()
    success = await backfiller.run_backfill()
    
    if success:
        logging.info("🎉 Backfill completed successfully")
        print(f"📋 Backfill log: {log_file}")
    else:
        logging.error("❌ Backfill failed")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)

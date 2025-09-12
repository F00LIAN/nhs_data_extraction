#!/usr/bin/env python3
"""
Temporary script to run city snapshots with addressRegion instead of postalCode
Usage: python run_city_snapshots_with_addressregion.py
"""

import asyncio
import os
import logging
import sys
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.price_tracker import PriceTracker

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'city_snapshots_addressregion_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)

class CitySnapshotRunner:
    def __init__(self):
        self.price_tracker = PriceTracker()
        
    async def run_snapshots(self):
        """Run city snapshot creation on existing data"""
        try:
            logging.info("🚀 Starting city snapshot creation with addressRegion...")
            
            # Connect to MongoDB
            await self.price_tracker.connect_to_mongodb()
            
            # Check data availability
            permanent_count = await self.price_tracker.price_history_permanent_collection.count_documents({})
            logging.info(f"📊 Found {permanent_count} records in price_history_permanent")
            
            if permanent_count == 0:
                logging.warning("⚠️ No data found in price_history_permanent collection")
                return
            
            # Check existing city snapshots
            existing_snapshots = await self.price_tracker.price_city_snapshot_collection.count_documents({})
            logging.info(f"🏙️ Found {existing_snapshots} existing city snapshots")
            
            # Run the city snapshot creation
            logging.info("🔄 Creating city snapshots...")
            await self.price_tracker._create_city_price_snapshots()
            
            # Verify results
            new_snapshots = await self.price_tracker.price_city_snapshot_collection.count_documents({})
            logging.info(f"✅ City snapshot creation completed!")
            logging.info(f"📈 Total city snapshots: {new_snapshots}")
            
            # Show sample of created snapshots
            sample_snapshots = await self.price_tracker.price_city_snapshot_collection.find({}).limit(3).to_list(length=3)
            for snapshot in sample_snapshots:
                city = snapshot.get("addressLocality", "Unknown")
                county = snapshot.get("county", "Unknown")
                region = snapshot.get("addressRegion", "Unknown")
                sfr_count = snapshot.get("current_active_metrics", {}).get("sfr", {}).get("count", 0)
                condo_count = snapshot.get("current_active_metrics", {}).get("condo", {}).get("count", 0)
                
                logging.info(f"🏠 {city}, {county}, {region}: {sfr_count} SFR, {condo_count} Condo")
            
            # Show historical daily averages sample
            if sample_snapshots:
                sample = sample_snapshots[0]
                historical = sample.get("historical_daily_averages", [])
                if historical:
                    latest = historical[-1]
                    logging.info(f"📅 Latest historical entry: {latest.get('date')} - SFR: {latest.get('sfr_listing_count')}, Condo: {latest.get('condo_listing_count')}")
            
        except Exception as e:
            logging.error(f"❌ Error running city snapshots: {e}")
            raise
        finally:
            # Close connection
            self.price_tracker.close_connection()
            logging.info("🔌 Database connection closed")

async def main():
    """Main execution function"""
    runner = CitySnapshotRunner()
    await runner.run_snapshots()

if __name__ == "__main__":
    print("🏙️ City Snapshot Runner with addressRegion")
    print("=" * 50)
    asyncio.run(main())

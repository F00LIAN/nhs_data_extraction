"""
Backfill Script: Fix Percentage Changes in Existing City Snapshots
Updates all existing price_city_snapshot documents with correct percentage calculations
using the new _calculate_city_metrics method
"""

import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List
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
        logging.FileHandler('backfill_percentages.log')
    ]
)

class PercentageBackfiller:
    """Backfill correct percentage changes for existing city snapshots"""
    
    def __init__(self):
        self.client = None
        self.db = None
        self.price_tracker = None
        
    async def connect_to_mongodb(self):
        """Connect to MongoDB"""
        try:
            uri = os.getenv("MONGO_DB_URI")
            if not uri:
                raise Exception("MONGO_DB_URI environment variable not found")
                
            self.client = AsyncIOMotorClient(uri)
            self.db = self.client['newhomesource']
            
            # Test connection
            await self.client.admin.command('ping')
            logging.info("Connected to MongoDB")
            
            # Initialize PriceTracker for its calculation methods
            self.price_tracker = PriceTracker()
            await self.price_tracker.connect_to_mongodb()
            
            return True
            
        except Exception as e:
            logging.error(f"❌ MongoDB connection failed: {e}")
            return False
    
    async def get_all_city_snapshots(self):
        """Get all existing city snapshots that need percentage fixes"""
        try:
            collection = self.db['price_city_snapshot']
            
            # Get all city snapshots
            snapshots = await collection.find({}).to_list(length=None)
            
            logging.info(f"Found {len(snapshots)} city snapshots to process")
            
            return snapshots
            
        except Exception as e:
            logging.error(f"❌ Error fetching city snapshots: {e}")
            return []
    
    async def calculate_correct_metrics(self, historical_daily_averages: List[Dict]) -> Dict:
        """Calculate correct metrics for all property types"""
        try:
            if not historical_daily_averages:
                logging.warning("⚠️ No historical data provided for metrics calculation")
                return {}
            
            # Calculate metrics for each property type using the new method
            sfr_metrics = await self.price_tracker._calculate_city_metrics(historical_daily_averages, "sfr")
            condo_metrics = await self.price_tracker._calculate_city_metrics(historical_daily_averages, "condo")
            overall_metrics = await self.price_tracker._calculate_city_metrics(historical_daily_averages, "overall")
            
            return {
                "sfr": sfr_metrics,
                "condo": condo_metrics,
                "overall": overall_metrics
            }
            
        except Exception as e:
            logging.error(f"❌ Error calculating correct metrics: {e}")
            return {}
    
    async def update_city_snapshot(self, snapshot: Dict) -> bool:
        """Update a single city snapshot with correct percentage calculations"""
        try:
            city_id = snapshot.get("city_id")
            city_name = snapshot.get("addressLocality", "Unknown")
            county = snapshot.get("county", "Unknown")
            
            logging.info(f"🔄 Processing {city_name}, {county} (ID: {city_id})")
            
            # Get historical daily averages
            historical_daily_averages = snapshot.get("historical_daily_averages", [])
            
            if not historical_daily_averages:
                logging.warning(f"⚠️ No historical data found for {city_name}, skipping")
                return False
            
            logging.info(f"📅 Found {len(historical_daily_averages)} historical entries")
            
            # Show current vs corrected calculation
            current_metrics = snapshot.get("current_active_metrics", {})
            current_sfr_change = current_metrics.get("sfr", {}).get("percent_changes", {}).get("1_day_change")
            
            # Calculate correct metrics
            correct_metrics = await self.calculate_correct_metrics(historical_daily_averages)
            
            if not correct_metrics:
                logging.error(f"❌ Failed to calculate metrics for {city_name}")
                return False
            
            # Log the correction
            corrected_sfr_change = correct_metrics.get("sfr", {}).get("percent_changes", {}).get("1_day_change")
            
            logging.info(f"📈 Correction for {city_name}:")
            logging.info(f"   SFR 1-day change: {current_sfr_change}% → {corrected_sfr_change}%")
            
            # Update the current_active_metrics with correct calculations
            updated_current_metrics = current_metrics.copy()
            
            # Update SFR metrics
            if "sfr" in updated_current_metrics:
                updated_current_metrics["sfr"]["moving_averages"] = correct_metrics["sfr"]["moving_averages"]
                updated_current_metrics["sfr"]["percent_changes"] = correct_metrics["sfr"]["percent_changes"]
            
            # Update Condo metrics
            if "condo" in updated_current_metrics:
                updated_current_metrics["condo"]["moving_averages"] = correct_metrics["condo"]["moving_averages"]
                updated_current_metrics["condo"]["percent_changes"] = correct_metrics["condo"]["percent_changes"]
            
            # Update Overall metrics
            if "overall" in updated_current_metrics:
                updated_current_metrics["overall"]["moving_averages"] = correct_metrics["overall"]["moving_averages"]
                updated_current_metrics["overall"]["percent_changes"] = correct_metrics["overall"]["percent_changes"]
            
            # Update the document in MongoDB
            collection = self.db['price_city_snapshot']
            
            result = await collection.update_one(
                {"city_id": city_id},
                {
                    "$set": {
                        "current_active_metrics": updated_current_metrics,
                        "percentage_backfill_applied": True,
                        "percentage_backfill_date": datetime.now(),
                        "percentage_backfill_version": "2.0"
                    }
                }
            )
            
            if result.modified_count > 0:
                logging.info(f"✅ Successfully updated {city_name}")
                return True
            else:
                logging.warning(f"⚠️ No changes made to {city_name}")
                return False
                
        except Exception as e:
            logging.error(f"❌ Error updating city snapshot: {e}")
            return False
    
    async def run_backfill(self):
        """Run the complete backfill process"""
        try:
            logging.info("🚀 Starting Percentage Calculation Backfill Process")
            
            # Connect to database
            if not await self.connect_to_mongodb():
                return False
            
            # Get all city snapshots
            snapshots = await self.get_all_city_snapshots()
            
            if not snapshots:
                logging.info("ℹ️ No snapshots found to process")
                return True
            
            # Process each snapshot
            success_count = 0
            failure_count = 0
            
            for i, snapshot in enumerate(snapshots, 1):
                city_name = snapshot.get("addressLocality", "Unknown")
                logging.info(f"\n📍 Processing {i}/{len(snapshots)}: {city_name}")
                
                success = await self.update_city_snapshot(snapshot)
                
                if success:
                    success_count += 1
                else:
                    failure_count += 1
                
                # Small delay to avoid overwhelming the database
                await asyncio.sleep(0.1)
            
            # Summary
            logging.info(f"\n📊 Backfill Complete!")
            logging.info(f"✅ Successfully updated: {success_count} snapshots")
            logging.info(f"❌ Failed to update: {failure_count} snapshots")
            logging.info(f"📈 Total processed: {len(snapshots)} snapshots")
            
            return failure_count == 0
            
        except Exception as e:
            logging.error(f"❌ Backfill process failed: {e}")
            return False
        
        finally:
            if self.client:
                self.client.close()
                logging.info("🔌 MongoDB connection closed")
    
    async def preview_changes(self):
        """Preview what changes would be made without updating"""
        try:
            logging.info("PREVIEW MODE: Showing changes that would be made")
            
            if not await self.connect_to_mongodb():
                return False
            
            snapshots = await self.get_all_city_snapshots()
            
            for snapshot in snapshots[:3]:  # Preview first 3 for demonstration
                city_name = snapshot.get("addressLocality", "Unknown")
                county = snapshot.get("county", "Unknown")
                
                logging.info(f"\nPreview: {city_name}, {county}")
                
                # Get current metrics
                current_metrics = snapshot.get("current_active_metrics", {})
                historical_data = snapshot.get("historical_daily_averages", [])
                
                if not historical_data:
                    logging.info("   No historical data available")
                    continue
                
                # Calculate what the correct metrics would be
                correct_metrics = await self.calculate_correct_metrics(historical_data)
                
                # Show comparison
                current_sfr_change = current_metrics.get("sfr", {}).get("percent_changes", {}).get("1_day_change")
                correct_sfr_change = correct_metrics.get("sfr", {}).get("percent_changes", {}).get("1_day_change")
                
                current_overall_change = current_metrics.get("overall", {}).get("percent_changes", {}).get("1_day_change")
                correct_overall_change = correct_metrics.get("overall", {}).get("percent_changes", {}).get("1_day_change")
                
                logging.info(f"   SFR 1-day change: {current_sfr_change}% -> {correct_sfr_change}%")
                logging.info(f"   Overall 1-day change: {current_overall_change}% -> {correct_overall_change}%")
                logging.info(f"   Historical entries: {len(historical_data)}")
            
            return True
            
        except Exception as e:
            logging.error(f"❌ Preview failed: {e}")
            return False
        
        finally:
            if self.client:
                self.client.close()

async def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Backfill correct percentage changes in city snapshots')
    parser.add_argument('--preview', action='store_true', help='Preview changes without applying them')
    parser.add_argument('--apply', action='store_true', help='Apply the backfill changes')
    
    args = parser.parse_args()
    
    if not args.preview and not args.apply:
        print("Please specify either --preview or --apply")
        return 1
    
    backfiller = PercentageBackfiller()
    
    try:
        if args.preview:
            success = await backfiller.preview_changes()
        else:
            success = await backfiller.run_backfill()
        
        return 0 if success else 1
        
    except Exception as e:
        logging.error(f"❌ Script failed: {e}")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)

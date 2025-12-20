"""
Backfill Script: Fix Listing Counts in City Price Snapshots
Updates historical_daily_averages entries where listing counts are 1 
with the earliest available count that is greater than 1.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

# Configure logging
log_filename = f"listing_count_backfill_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'logs/{log_filename}')
    ]
)

class ListingCountBackfiller:
    """Backfill listing counts in price_city_snapshot historical daily averages"""
    
    def __init__(self):
        self.client = None
        self.db = None
        
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
            logging.info("✅ Connected to MongoDB successfully")
            
        except Exception as e:
            logging.error(f"❌ Failed to connect to MongoDB: {e}")
            raise
    
    async def close_connection(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logging.info("📝 Closed MongoDB connection")
    
    def find_earliest_valid_counts(self, historical_averages: List[Dict]) -> Dict[str, int]:
        """
        Find the earliest counts > 1 for sfr, condo, and overall
        Returns a dictionary with the earliest valid counts
        """
        earliest_counts = {
            'sfr_listing_count': None,
            'condo_listing_count': None, 
            'overall_listing_count': None
        }
        
        # Sort by date to ensure we process chronologically
        sorted_averages = sorted(historical_averages, key=lambda x: x['date'])
        
        for entry in sorted_averages:
            # Check SFR count
            if (earliest_counts['sfr_listing_count'] is None and 
                entry.get('sfr_listing_count', 0) > 1):
                earliest_counts['sfr_listing_count'] = entry['sfr_listing_count']
            
            # Check condo count
            if (earliest_counts['condo_listing_count'] is None and 
                entry.get('condo_listing_count', 0) > 1):
                earliest_counts['condo_listing_count'] = entry['condo_listing_count']
            
            # Check overall count
            if (earliest_counts['overall_listing_count'] is None and 
                entry.get('overall_listing_count', 0) > 1):
                earliest_counts['overall_listing_count'] = entry['overall_listing_count']
        
        return earliest_counts
    
    def needs_backfill(self, historical_averages: List[Dict]) -> bool:
        """Check if document needs backfill (has any counts equal to 1)"""
        for entry in historical_averages:
            if (entry.get('sfr_listing_count') == 1 or 
                entry.get('condo_listing_count') == 1 or 
                entry.get('overall_listing_count') == 1):
                return True
        return False
    
    def apply_backfill(self, historical_averages: List[Dict], earliest_counts: Dict) -> List[Dict]:
        """Apply backfill to historical averages"""
        updated_averages = []
        changes_made = 0
        
        for entry in historical_averages:
            updated_entry = entry.copy()
            
            # Backfill SFR count if it's 1 and we have a valid replacement
            if (entry.get('sfr_listing_count') == 1 and 
                earliest_counts['sfr_listing_count'] is not None):
                updated_entry['sfr_listing_count'] = earliest_counts['sfr_listing_count']
                changes_made += 1
            
            # Backfill condo count if it's 1 and we have a valid replacement
            if (entry.get('condo_listing_count') == 1 and 
                earliest_counts['condo_listing_count'] is not None):
                updated_entry['condo_listing_count'] = earliest_counts['condo_listing_count']
                changes_made += 1
            
            # Backfill overall count if it's 1 and we have a valid replacement
            if (entry.get('overall_listing_count') == 1 and 
                earliest_counts['overall_listing_count'] is not None):
                updated_entry['overall_listing_count'] = earliest_counts['overall_listing_count']
                changes_made += 1
            
            updated_averages.append(updated_entry)
        
        return updated_averages, changes_made
    
    async def backfill_single_document(self, doc: Dict, dry_run: bool = True) -> Dict:
        """Process a single document for backfill"""
        city_id = doc.get('city_id')
        city_name = doc.get('addressLocality', 'Unknown')
        region = doc.get('addressRegion', 'Unknown')
        historical_averages = doc.get('historical_daily_averages', [])
        
        result = {
            'city_id': city_id,
            'city_name': city_name,
            'region': region,
            'processed': False,
            'changes_made': 0,
            'error': None
        }
        
        try:
            # Check if backfill is needed
            if not self.needs_backfill(historical_averages):
                logging.info(f"⏭️  No backfill needed for {city_name}, {region}")
                return result
            
            # Find earliest valid counts
            earliest_counts = self.find_earliest_valid_counts(historical_averages)
            
            # Log what we found
            logging.info(f"🔍 {city_name}, {region} - Earliest valid counts: "
                        f"SFR={earliest_counts['sfr_listing_count']}, "
                        f"Condo={earliest_counts['condo_listing_count']}, "
                        f"Overall={earliest_counts['overall_listing_count']}")
            
            # Apply backfill
            updated_averages, changes_made = self.apply_backfill(historical_averages, earliest_counts)
            
            if changes_made > 0:
                result['changes_made'] = changes_made
                result['processed'] = True
                
                if not dry_run:
                    # Update the document
                    await self.db['price_city_snapshot'].update_one(
                        {'_id': doc['_id']},
                        {
                            '$set': {
                                'historical_daily_averages': updated_averages,
                                'listing_count_backfill_applied': True,
                                'listing_count_backfill_date': datetime.now(),
                                'listing_count_backfill_version': '1.0'
                            }
                        }
                    )
                    logging.info(f"✅ Updated {city_name}, {region} - {changes_made} changes applied")
                else:
                    logging.info(f"🔍 DRY RUN: Would update {city_name}, {region} - {changes_made} changes")
            else:
                logging.info(f"⏭️  No changes needed for {city_name}, {region}")
                
        except Exception as e:
            logging.error(f"❌ Error processing {city_name}, {region}: {e}")
            result['error'] = str(e)
        
        return result
    
    async def run_backfill(self, dry_run: bool = True):
        """Run the backfill process"""
        try:
            logging.info("=" * 60)
            logging.info(f"🚀 Starting listing count backfill - Mode: {'DRY RUN' if dry_run else 'LIVE UPDATE'}")
            logging.info("=" * 60)
            
            # Get all city snapshot documents
            cursor = self.db['price_city_snapshot'].find({})
            documents = await cursor.to_list(None)
            
            logging.info(f"📊 Found {len(documents)} snapshots to check for backfill")
            
            results = []
            processed_count = 0
            updated_count = 0
            total_changes = 0
            
            for doc in documents:
                result = await self.backfill_single_document(doc, dry_run)
                results.append(result)
                
                if result['processed']:
                    processed_count += 1
                    if result['changes_made'] > 0:
                        updated_count += 1
                        total_changes += result['changes_made']
            
            # Summary
            logging.info("=" * 60)
            logging.info(f"📈 BACKFILL SUMMARY:")
            logging.info(f"   Total documents checked: {len(documents)}")
            logging.info(f"   Documents needing backfill: {processed_count}")
            logging.info(f"   Documents updated: {updated_count}")
            logging.info(f"   Total individual changes: {total_changes}")
            logging.info("=" * 60)
            
            return results
            
        except Exception as e:
            logging.error(f"❌ Error in backfill process: {e}")
            raise

async def main():
    """Main execution function"""
    backfiller = ListingCountBackfiller()
    
    try:
        await backfiller.connect_to_mongodb()
        
        # Run in dry run mode first
        logging.info("🔍 Running in DRY RUN mode first...")
        dry_results = await backfiller.run_backfill(dry_run=True)
        
        # Ask for confirmation to proceed with actual updates
        proceed = input("\n🤔 Do you want to proceed with the actual updates? (y/N): ")
        
        if proceed.lower() in ['y', 'yes']:
            logging.info("🚀 Running with LIVE UPDATES...")
            live_results = await backfiller.run_backfill(dry_run=False)
        else:
            logging.info("⏹️  Backfill cancelled by user")
            
    except Exception as e:
        logging.error(f"❌ Script failed: {e}")
    finally:
        await backfiller.close_connection()

if __name__ == "__main__":
    asyncio.run(main())

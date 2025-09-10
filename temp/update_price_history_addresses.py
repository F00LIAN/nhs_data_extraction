#!/usr/bin/env python3
"""
Temporary Script: Update Price History Addresses
Updates existing price_history_permanent records with address data from communitydata collection.
"""

import asyncio
import os
import logging
from datetime import datetime
from typing import Dict, List
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class PriceHistoryAddressUpdater:
    """Updates price_history_permanent records with address data from communitydata"""
    
    def __init__(self):
        self.client = None
        self.db = None
        self.communitydata_collection = None
        self.price_history_permanent_collection = None
        
    async def connect_to_mongodb(self):
        """Connect to MongoDB"""
        try:
            uri = os.getenv("MONGO_DB_URI")
            if not uri:
                raise ValueError("MONGO_DB_URI environment variable is required")
                
            self.client = AsyncIOMotorClient(uri)
            self.db = self.client['newhomesource']
            
            self.communitydata_collection = self.db['communitydata']
            self.price_history_permanent_collection = self.db['price_history_permanent']
            
            logging.info("✅ Connected to MongoDB")
            return True
        except Exception as e:
            logging.error(f"❌ MongoDB connection failed: {e}")
            return False
    
    async def update_price_history_addresses(self, dry_run: bool = True):
        """
        Update price_history_permanent records with address data from communitydata
        
        Args:
            dry_run (bool): If True, only log what would be updated without making changes
        """
        try:
            logging.info(f"🚀 Starting address update process (dry_run={dry_run})")
            
            # Get all price_history_permanent records
            price_records = await self.price_history_permanent_collection.find({}).to_list(length=None)
            logging.info(f"📊 Found {len(price_records)} price history records")
            
            updated_count = 0
            not_found_count = 0
            already_complete_count = 0
            error_count = 0
            not_found_community_ids = []  # Track missing community IDs
            
            for record in price_records:
                try:
                    community_id = record.get("community_id")
                    permanent_id = record.get("permanent_property_id")
                    
                    if not community_id:
                        logging.warning(f"⚠️ No community_id in record {permanent_id}")
                        error_count += 1
                        continue
                    
                    # Check if address is already complete
                    current_address = record.get("address", {})
                    if self._is_address_complete(current_address):
                        logging.debug(f"✅ Address already complete for {community_id}")
                        already_complete_count += 1
                        continue
                    
                    # Find the community in communitydata collection
                    address_data = await self._find_community_address(community_id)
                    
                    if not address_data:
                        logging.warning(f"⚠️ No address data found for community: {community_id}")
                        not_found_count += 1
                        not_found_community_ids.append(community_id)  # Track this missing ID
                        continue
                    
                    # Update the record
                    if dry_run:
                        logging.info(f"🔍 DRY RUN - Would update {community_id} with address: {address_data}")
                    else:
                        await self._update_record_address(permanent_id, address_data)
                        logging.info(f"✅ Updated address for {community_id}")
                    
                    updated_count += 1
                    
                except Exception as e:
                    logging.error(f"❌ Error processing record {record.get('permanent_property_id', 'unknown')}: {e}")
                    error_count += 1
            
            # Summary
            logging.info(f"📋 Update Summary:")
            logging.info(f"   ✅ Updated: {updated_count}")
            logging.info(f"   ✅ Already complete: {already_complete_count}")
            logging.info(f"   ⚠️ Not found in communitydata: {not_found_count}")
            logging.info(f"   ❌ Errors: {error_count}")
            logging.info(f"   📊 Total processed: {len(price_records)}")
            
            # Log missing community IDs
            if not_found_community_ids:
                logging.warning(f"🔍 Missing Community IDs ({len(not_found_community_ids)} total):")
                for i, missing_id in enumerate(not_found_community_ids[:20], 1):  # Show first 20
                    logging.warning(f"   {i:2d}. {missing_id}")
                
                if len(not_found_community_ids) > 20:
                    logging.warning(f"   ... and {len(not_found_community_ids) - 20} more missing IDs")
                
                # Write full list to file
                missing_ids_file = "missing_community_ids.txt"
                with open(missing_ids_file, 'w') as f:
                    f.write(f"Missing Community IDs - Generated on {datetime.now()}\n")
                    f.write(f"Total missing: {len(not_found_community_ids)}\n\n")
                    for missing_id in not_found_community_ids:
                        f.write(f"{missing_id}\n")
                
                logging.info(f"📄 Full list of missing community IDs saved to: {missing_ids_file}")
            else:
                logging.info("✅ All community IDs were found in communitydata collection!")
            
            if dry_run:
                logging.info("🔍 This was a DRY RUN - no changes were made")
                logging.info("🔍 Run with dry_run=False to apply changes")
            
            return {
                "total_processed": len(price_records),
                "updated": updated_count,
                "already_complete": already_complete_count,
                "not_found": not_found_count,
                "errors": error_count,
                "missing_community_ids": not_found_community_ids
            }
            
        except Exception as e:
            logging.error(f"❌ Error in update process: {e}")
            return {}
    
    def _is_address_complete(self, address: Dict) -> bool:
        """Check if address data is already complete"""
        if not isinstance(address, dict):
            return False
        
        # Check if key address fields have meaningful data (not empty strings)
        key_fields = ["county", "addressLocality", "addressRegion"]
        for field in key_fields:
            value = address.get(field, "")
            if value and value.strip():
                return True
        
        return False
    
    async def _find_community_address(self, community_id: str) -> Dict:
        """Find address data for a community_id in communitydata collection"""
        try:
            # Search for the community across all community documents
            cursor = self.communitydata_collection.find({
                "community_data.communities.community_id": community_id
            })
            
            async for doc in cursor:
                communities = doc.get("community_data", {}).get("communities", [])
                for community in communities:
                    if community.get("community_id") == community_id:
                        address = community.get("address", {})
                        if isinstance(address, dict) and address:
                            # Transform to match price_history_permanent structure
                            return {
                                "county": address.get("county", ""),
                                "addressLocality": address.get("addressLocality", ""),
                                "addressRegion": address.get("addressRegion", ""),
                                "streetAddress": address.get("streetAddress", ""),
                                "postalCode": address.get("postalCode", "")
                            }
            
            return {}
            
        except Exception as e:
            logging.error(f"❌ Error finding address for {community_id}: {e}")
            return {}
    
    async def _update_record_address(self, permanent_id: str, address_data: Dict):
        """Update a price_history_permanent record with new address data"""
        try:
            result = await self.price_history_permanent_collection.update_one(
                {"permanent_property_id": permanent_id},
                {
                    "$set": {
                        "address": address_data,
                        "last_updated": datetime.now()
                    }
                }
            )
            
            if result.modified_count == 0:
                logging.warning(f"⚠️ No record updated for permanent_id: {permanent_id}")
            
        except Exception as e:
            logging.error(f"❌ Error updating record {permanent_id}: {e}")
            raise
    
    async def get_update_statistics(self):
        """Get statistics about what needs to be updated"""
        try:
            # Count total records
            total_records = await self.price_history_permanent_collection.count_documents({})
            
            # Count records with empty addresses
            empty_address_count = await self.price_history_permanent_collection.count_documents({
                "$or": [
                    {"address": {"$exists": False}},
                    {"address.county": {"$in": ["", None]}},
                    {"address.addressLocality": {"$in": ["", None]}},
                    {"address.addressRegion": {"$in": ["", None]}}
                ]
            })
            
            logging.info(f"📊 Update Statistics:")
            logging.info(f"   📊 Total price_history_permanent records: {total_records}")
            logging.info(f"   🔍 Records needing address updates: {empty_address_count}")
            logging.info(f"   ✅ Records with complete addresses: {total_records - empty_address_count}")
            
            return {
                "total_records": total_records,
                "needs_update": empty_address_count,
                "complete": total_records - empty_address_count
            }
            
        except Exception as e:
            logging.error(f"❌ Error getting statistics: {e}")
            return {}
    
    async def analyze_missing_community_ids(self):
        """
        Analyze missing community IDs to understand patterns and provide insights
        """
        try:
            logging.info("🔍 Analyzing missing community IDs...")
            
            # Get all community IDs from price_history_permanent
            price_community_ids = set()
            async for record in self.price_history_permanent_collection.find({}, {"community_id": 1}):
                community_id = record.get("community_id")
                if community_id:
                    price_community_ids.add(community_id)
            
            # Get all community IDs from communitydata
            communitydata_ids = set()
            async for doc in self.communitydata_collection.find({}, {"community_data.communities.community_id": 1}):
                communities = doc.get("community_data", {}).get("communities", [])
                for community in communities:
                    community_id = community.get("community_id")
                    if community_id:
                        communitydata_ids.add(community_id)
            
            # Find missing IDs
            missing_ids = price_community_ids - communitydata_ids
            
            logging.info(f"📊 Missing Community ID Analysis:")
            logging.info(f"   📈 Total community IDs in price_history_permanent: {len(price_community_ids)}")
            logging.info(f"   📈 Total community IDs in communitydata: {len(communitydata_ids)}")
            logging.info(f"   ❌ Missing from communitydata: {len(missing_ids)}")
            logging.info(f"   ✅ Present in both collections: {len(price_community_ids & communitydata_ids)}")
            
            if missing_ids:
                # Analyze patterns in missing IDs
                patterns = {}
                for missing_id in missing_ids:
                    # Extract domain pattern
                    if "newhomesource.com" in missing_id:
                        if "/plan/" in missing_id:
                            pattern = "plan"
                        elif "/specdetail/" in missing_id:
                            pattern = "specdetail"
                        elif "/community/" in missing_id:
                            pattern = "community"
                        elif "/masterplan/" in missing_id:
                            pattern = "masterplan"
                        else:
                            pattern = "other"
                    else:
                        pattern = "non-newhomesource"
                    
                    patterns[pattern] = patterns.get(pattern, 0) + 1
                
                logging.info(f"📊 Missing ID Patterns:")
                for pattern, count in patterns.items():
                    logging.info(f"   📊 {pattern}: {count} missing IDs")
                
                # Sample missing IDs
                sample_missing = list(missing_ids)[:10]
                logging.warning(f"🔍 Sample Missing Community IDs:")
                for i, missing_id in enumerate(sample_missing, 1):
                    logging.warning(f"   {i:2d}. {missing_id}")
                
                # Write detailed analysis to file
                analysis_file = "missing_community_ids_analysis.txt"
                with open(analysis_file, 'w') as f:
                    f.write(f"Missing Community IDs Analysis - Generated on {datetime.now()}\n")
                    f.write(f"{'='*60}\n\n")
                    f.write(f"Total community IDs in price_history_permanent: {len(price_community_ids)}\n")
                    f.write(f"Total community IDs in communitydata: {len(communitydata_ids)}\n")
                    f.write(f"Missing from communitydata: {len(missing_ids)}\n\n")
                    
                    f.write("Pattern Analysis:\n")
                    for pattern, count in patterns.items():
                        f.write(f"  {pattern}: {count} missing IDs\n")
                    f.write("\n")
                    
                    f.write("All Missing Community IDs:\n")
                    f.write("-" * 40 + "\n")
                    for missing_id in sorted(missing_ids):
                        f.write(f"{missing_id}\n")
                
                logging.info(f"📄 Detailed analysis saved to: {analysis_file}")
            
            return {
                "total_price_history": len(price_community_ids),
                "total_communitydata": len(communitydata_ids),
                "missing_count": len(missing_ids),
                "missing_ids": list(missing_ids),
                "patterns": patterns if missing_ids else {}
            }
            
        except Exception as e:
            logging.error(f"❌ Error analyzing missing community IDs: {e}")
            return {}

    def close_connection(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logging.info("🔌 MongoDB connection closed")


async def main():
    """Main function to run the address update process"""
    updater = PriceHistoryAddressUpdater()
    
    try:
        # Connect to database
        if not await updater.connect_to_mongodb():
            logging.error("❌ Failed to connect to MongoDB")
            return
        
        # Get statistics first
        await updater.get_update_statistics()
        
        # Analyze missing community IDs
        await updater.analyze_missing_community_ids()
        
        # Run dry run first
        logging.info("🔍 Running DRY RUN first...")
        dry_run_results = await updater.update_price_history_addresses(dry_run=True)
        
        if dry_run_results.get("updated", 0) > 0:
            # Ask user for confirmation
            response = input(f"\n🤔 Found {dry_run_results['updated']} records to update. Proceed with actual update? (y/N): ")
            
            if response.lower() == 'y':
                logging.info("🚀 Running ACTUAL UPDATE...")
                actual_results = await updater.update_price_history_addresses(dry_run=False)
                logging.info("✅ Address update process completed!")
            else:
                logging.info("❌ Update cancelled by user")
        else:
            logging.info("ℹ️ No records need updating")
            
    except Exception as e:
        logging.error(f"❌ Error in main process: {e}")
    finally:
        updater.close_connection()


if __name__ == "__main__":
    asyncio.run(main())

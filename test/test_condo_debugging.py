"""
Debugging Test for Condominium Issues in City Snapshots
This test connects to your actual database to diagnose why condos aren't showing up
"""

import asyncio
import logging
import os
from collections import Counter
from dotenv import load_dotenv
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.price_tracker import PriceTracker

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CondoDebuggingTest:
    """Debug condominium handling in city snapshots"""
    
    def __init__(self):
        self.price_tracker = PriceTracker()
    
    async def run_diagnosis(self):
        """Run complete diagnosis of condominium data"""
        try:
            await self.price_tracker.connect_to_mongodb()
            
            print("🔍 CONDOMINIUM DEBUGGING ANALYSIS")
            print("=" * 50)
            
            # Step 1: Check what accommodation categories exist
            await self._check_accommodation_categories()
            
            # Step 2: Check condo records specifically
            await self._check_condo_records()
            
            # Step 3: Test the aggregation pipeline directly
            await self._test_aggregation_pipeline()
            
            # Step 4: Test the city snapshot creation with debugging
            await self._test_city_snapshot_creation()
            
        except Exception as e:
            logger.error(f"❌ Error during diagnosis: {e}")
        finally:
            self.price_tracker.close_connection()
    
    async def _check_accommodation_categories(self):
        """Check what accommodation categories exist in the database"""
        print("\n📊 STEP 1: Accommodation Categories Analysis")
        print("-" * 40)
        
        # Get all unique accommodation categories
        pipeline = [
            {"$group": {"_id": "$accommodationCategory", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        categories = await self.price_tracker.price_history_permanent_collection.aggregate(pipeline).to_list(length=None)
        
        print("Accommodation Categories in price_history_permanent:")
        for cat in categories:
            category_name = cat["_id"] or "NULL/MISSING"
            print(f"  • '{category_name}': {cat['count']} records")
        
        # Check for variations that might not match exactly
        print("\nChecking for potential string variations...")
        all_categories = [cat["_id"] for cat in categories if cat["_id"]]
        
        condo_variations = [cat for cat in all_categories if cat and "condo" in cat.lower()]
        if condo_variations:
            print(f"  🏢 Condo-related variations found: {condo_variations}")
        else:
            print("  ⚠️ No condo-related categories found!")
        
        sfr_variations = [cat for cat in all_categories if cat and "family" in cat.lower()]
        if sfr_variations:
            print(f"  🏠 Single Family variations found: {sfr_variations}")
    
    async def _check_condo_records(self):
        """Check specific condominium records"""
        print("\n🏢 STEP 2: Condominium Records Analysis")
        print("-" * 40)
        
        # Find records with "Condominium" exactly
        exact_condos = await self.price_tracker.price_history_permanent_collection.find({
            "accommodationCategory": "Condominium"
        }).to_list(length=10)  # Limit to 10 for display
        
        print(f"Records with exact 'Condominium' match: {len(exact_condos)}")
        
        if exact_condos:
            print("Sample condo records:")
            for i, condo in enumerate(exact_condos[:3]):  # Show first 3
                print(f"  {i+1}. Community: {condo.get('community_name', 'Unknown')}")
                print(f"     Status: {condo.get('listing_status', 'Unknown')}")
                print(f"     Price: ${condo.get('aggregated_metrics', {}).get('most_recent_price', 'Unknown')}")
                print(f"     City: {condo.get('address', {}).get('addressLocality', 'Unknown')}")
                print(f"     County: {condo.get('address', {}).get('county', 'Unknown')}")
                print()
        
        # Check status distribution for condos
        status_pipeline = [
            {"$match": {"accommodationCategory": "Condominium"}},
            {"$group": {"_id": "$listing_status", "count": {"$sum": 1}}}
        ]
        
        condo_statuses = await self.price_tracker.price_history_permanent_collection.aggregate(status_pipeline).to_list(length=None)
        
        print("Condominium listing status distribution:")
        for status in condo_statuses:
            status_name = status["_id"] or "NULL/MISSING"
            print(f"  • {status_name}: {status['count']} condos")
    
    async def _test_aggregation_pipeline(self):
        """Test the actual aggregation pipeline used in city snapshots"""
        print("\n🔄 STEP 3: Aggregation Pipeline Testing")
        print("-" * 40)
        
        # Use the same pipeline as in _create_city_price_snapshots
        pipeline = [
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
                            "listing_status": "$listing_status",
                            "current_price": "$aggregated_metrics.most_recent_price",
                            "community_name": "$community_name"
                        }
                    }
                }
            }
        ]
        
        city_aggregates = await self.price_tracker.price_history_permanent_collection.aggregate(pipeline).to_list(length=None)
        
        print(f"Total cities found: {len(city_aggregates)}")
        
        # Analyze each city for condos
        cities_with_condos = 0
        total_condos = 0
        
        for city_data in city_aggregates:
            city_info = city_data["_id"]
            city_name = city_info.get("city", "Unknown")
            county = city_info.get("county", "Unknown")
            properties = city_data["properties"]
            
            # Count property types
            property_types = [p.get("accommodation_category") for p in properties]
            type_counts = Counter(property_types)
            
            # Active properties only
            active_properties = [p for p in properties if p.get("listing_status") == "active"]
            active_condos = [p for p in active_properties if p.get("accommodation_category") == "Condominium"]
            
            if active_condos:
                cities_with_condos += 1
                total_condos += len(active_condos)
                print(f"\n  🏢 {city_name}, {county}:")
                print(f"     Total properties: {len(properties)}")
                print(f"     Active properties: {len(active_properties)}")
                print(f"     Active condos: {len(active_condos)}")
                print(f"     Property type breakdown: {dict(type_counts)}")
                
                # Show condo details
                for condo in active_condos[:2]:  # Show first 2
                    price = condo.get("current_price", "Unknown")
                    name = condo.get("community_name", "Unknown")
                    print(f"       • {name}: ${price}")
        
        print(f"\nSUMMARY:")
        print(f"  Cities with active condos: {cities_with_condos}")
        print(f"  Total active condos: {total_condos}")
    
    async def _test_city_snapshot_creation(self):
        """Test city snapshot creation with enhanced debugging"""
        print("\n🏙️ STEP 4: City Snapshot Creation Test")
        print("-" * 40)
        
        print("Running city snapshot creation with debugging...")
        
        # This will use our updated _create_city_price_snapshots method with debug logs
        await self.price_tracker._create_city_price_snapshots()
        
        print("✅ City snapshot creation completed. Check logs above for debug output.")


async def main():
    """Run the diagnosis"""
    diagnostic = CondoDebuggingTest()
    await diagnostic.run_diagnosis()


if __name__ == "__main__":
    asyncio.run(main())

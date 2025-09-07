"""
Stage 2 Data Processor Module
Handles change detection, database operations, and price tracking
"""

import logging
import os
from datetime import datetime
from typing import Dict, List, Set, Tuple
from motor.motor_asyncio import AsyncIOMotorClient
try:
    from ..validation.stage_two_structure_validation import validate_community_document_structure
except ImportError:
    # Fallback for when run as script (GitHub Actions)
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from validation.stage_two_structure_validation import validate_community_document_structure


class DataProcessor:
    """Processes community data changes and manages database operations"""
    
    def __init__(self):
        self.uri = os.getenv("MONGO_DB_URI")
        if not self.uri:
            raise ValueError("MONGO_DB_URI environment variable is required")
    
    async def connect_to_mongodb(self):
        """
        Connect to MongoDB collections.
        
        Input: None
        Output: Tuple[client, homepagedata_collection, communitydata_collection, temp_collection]
        Description: Establishes async connections to required MongoDB collections
        """
        try:
            client = AsyncIOMotorClient(self.uri)
            db = client['newhomesource']
            return (
                client,
                db['homepagedata'],
                db['communitydata'],
                db['temphtml']
            )
        except Exception as e:
            raise Exception(f"Failed to connect to MongoDB: {e}")
    
    async def get_existing_community_data(self, communitydata_collection) -> Dict:
        """
        Get existing community data for change detection.
        
        Input: communitydata_collection (AsyncIOMotorCollection)
        Output: Dict mapping listing_id to existing community data
        Description: Retrieves current database state for comparison with new scrape data
        """
        try:
            existing_data = {}
            cursor = communitydata_collection.find({}, {"listing_id": 1, "community_data": 1, "scraped_at": 1})
            
            async for doc in cursor:
                listing_id = doc.get("listing_id")
                if listing_id:
                    existing_data[listing_id] = {
                        "community_data": doc.get("community_data", {}),
                        "scraped_at": doc.get("scraped_at"),
                        "communities": doc.get("community_data", {}).get("communities", [])
                    }
            
            return existing_data
        except Exception as e:
            logging.error(f"❌ Error getting existing community data: {e}")
            return {}
    
    async def process_community_changes(self, listing_id: str, new_communities: List[Dict], 
                                      existing_data: Dict, communitydata_collection) -> Tuple[str, Dict]:
        """
        Process community changes and update database.
        
        Input: listing_id (str), new_communities (List[Dict]), existing_data (Dict), collection
        Output: Tuple[change_type (str), change_stats (Dict)]
        Description: Detects changes between old and new data, updates database, returns change summary
        """
        try:
            if not existing_data:
                # New listing - save all communities
                community_doc = {
                    "listing_id": listing_id,
                    "scraped_at": datetime.now().isoformat(),
                    "community_data": {"communities": new_communities},
                    "total_communities_found": len(new_communities),
                    "listing_status": "new",
                    "last_updated": datetime.now()
                }
                
                # Validate document structure before database insertion
                if not validate_community_document_structure(community_doc):
                    logging.warning(f"⚠️ Skipping invalid community document structure for listing: {listing_id}")
                    return "error", {}
                
                await communitydata_collection.update_one(
                    {"listing_id": listing_id},
                    {"$set": community_doc},
                    upsert=True
                )
                
                return "new", {"new": len(new_communities)}
            
            # Compare existing vs new communities
            existing_communities = existing_data.get("communities", [])
            changes = self._detect_community_changes(existing_communities, new_communities)
            
            if changes["has_changes"]:
                # Update with changes
                community_doc = {
                    "listing_id": listing_id,
                    "scraped_at": datetime.now().isoformat(),
                    "community_data": {"communities": new_communities},
                    "total_communities_found": len(new_communities),
                    "listing_status": "updated",
                    "last_updated": datetime.now(),
                    "previous_scraped_at": existing_data.get("scraped_at"),
                    "change_summary": {
                        "new_communities": changes["new_count"],
                        "updated_communities": changes["updated_count"],
                        "removed_communities": changes["removed_count"],
                        "total_changes": changes["total_changes"]
                    }
                }
                
                # Validate document structure before database insertion
                if not validate_community_document_structure(community_doc):
                    logging.warning(f"⚠️ Skipping invalid community document structure for listing: {listing_id}")
                    return "error", {}
                
                await communitydata_collection.update_one(
                    {"listing_id": listing_id},
                    {"$set": community_doc},
                    upsert=True
                )
                
                logging.info(f"🔄 {listing_id}: {changes['total_changes']} changes detected")
                return "updated", {
                    "new": changes["new_count"],
                    "updated": changes["updated_count"],
                    "unchanged": len(new_communities) - changes["new_count"] - changes["updated_count"]
                }
            else:
                # No changes - just update timestamp
                await communitydata_collection.update_one(
                    {"listing_id": listing_id},
                    {"$set": {
                        "scraped_at": datetime.now().isoformat(),
                        "listing_status": "active"
                    }}
                )
                
                return "unchanged", {"unchanged": len(new_communities)}
                
        except Exception as e:
            logging.error(f"❌ Error processing community changes for {listing_id}: {e}")
            return "error", {}
    
    def _detect_community_changes(self, existing_communities: List[Dict], new_communities: List[Dict]) -> Dict:
        """
        Detect changes between existing and new community data.
        
        Input: existing_communities (List[Dict]), new_communities (List[Dict])
        Output: Dict with change statistics and flags
        Description: Compares community data to identify new, updated, and removed communities
        """
        try:
            # Create lookups by community_id
            existing_lookup = {c.get("community_id"): c for c in existing_communities if c.get("community_id")}
            new_lookup = {c.get("community_id"): c for c in new_communities if c.get("community_id")}
            
            existing_ids = set(existing_lookup.keys())
            new_ids = set(new_lookup.keys())
            
            # Detect changes
            new_community_ids = new_ids - existing_ids
            removed_community_ids = existing_ids - new_ids
            common_community_ids = existing_ids & new_ids
            
            updated_count = 0
            
            # Check for updates in common communities
            for community_id in common_community_ids:
                existing_community = existing_lookup[community_id]
                new_community = new_lookup[community_id]
                
                if self._has_community_changed(existing_community, new_community):
                    updated_count += 1
            
            total_changes = len(new_community_ids) + len(removed_community_ids) + updated_count
            
            return {
                "has_changes": total_changes > 0,
                "new_count": len(new_community_ids),
                "removed_count": len(removed_community_ids),
                "updated_count": updated_count,
                "total_changes": total_changes,
                "new_ids": list(new_community_ids),
                "removed_ids": list(removed_community_ids)
            }
            
        except Exception as e:
            logging.error(f"❌ Error detecting community changes: {e}")
            return {"has_changes": False, "new_count": 0, "removed_count": 0, "updated_count": 0, "total_changes": 0}
    
    def _has_community_changed(self, existing_community: Dict, new_community: Dict) -> bool:
        """
        Check if individual community has changed.
        
        Input: existing_community (Dict), new_community (Dict)
        Output: bool - True if changes detected
        Description: Compares key fields to determine if community data has been updated
        """
        compare_fields = ["name", "price", "build_status", "build_type", "url"]
        
        for field in compare_fields:
            existing_value = existing_community.get(field)
            new_value = new_community.get(field)
            
            # Normalize price for comparison
            if field == "price":
                try:
                    existing_value = float(existing_value) if existing_value else 0
                    new_value = float(new_value) if new_value else 0
                except:
                    pass
            
            if existing_value != new_value:
                return True
        
        return False
    
    async def handle_removed_listings(self, existing_listing_ids: Set, processed_listing_ids: Set, 
                                    communitydata_collection):
        """
        Handle listings that were removed from current scrape.
        
        Input: existing_listing_ids (Set), processed_listing_ids (Set), collection
        Output: None
        Description: Archives listings that exist in DB but not in current scrape (with safety checks)
        """
        try:
            removed_listing_ids = existing_listing_ids - processed_listing_ids
            
            if not removed_listing_ids:
                logging.info("✅ No removed community listings detected")
                return
            
            # Safety check - prevent mass removal due to scraping issues
            removal_percentage = len(removed_listing_ids) / len(existing_listing_ids) if existing_listing_ids else 0
            
            if removal_percentage > 0.5:  # >50% would be removed
                logging.error(f"🚨 SAFETY CHECK: {removal_percentage:.1%} removal rate detected - skipping mass removal")
                logging.error("🚨 Manual investigation required")
                return
            
            logging.info(f"🗑️ Archiving {len(removed_listing_ids)} removed community listings")
            
            # Mark as archived (don't delete - keep for historical analysis)
            for listing_id in removed_listing_ids:
                await communitydata_collection.update_one(
                    {"listing_id": listing_id},
                    {"$set": {
                        "listing_status": "archived",
                        "archived_at": datetime.now(),
                        "archive_reason": "missing from current Stage 2 scrape"
                    }}
                )
            
            logging.info(f"📦 Successfully archived {len(removed_listing_ids)} community listings")
            
        except Exception as e:
            logging.error(f"❌ Error handling removed community listings: {e}")
    
    async def capture_price_snapshots(self):
        """
        Capture price snapshots after Stage 2 completion.
        
        Input: None
        Output: None
        Description: Triggers price tracking system to record current price data
        """
        try:
            logging.info("💰 Starting Stage 2 price snapshot capture...")
            
            # Import here to avoid circular imports
            import sys
            import os
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            from shared.price_tracker import PriceTracker
            
            price_tracker = PriceTracker()
            await price_tracker.connect_to_mongodb()
            await price_tracker.capture_price_snapshots_from_stage2()
            price_tracker.close_connection()
            
            logging.info("✅ Stage 2 price tracking completed successfully")
        except Exception as e:
            logging.error(f"❌ Error capturing price snapshots: {e}")
            # Don't fail the entire Stage 2 process if price tracking fails
            logging.warning("⚠️ Stage 2 will continue despite price tracking failure")

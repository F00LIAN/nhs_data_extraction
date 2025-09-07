"""
Price Tracking Service
Handles price snapshot creation and permanent storage consolidation
"""

import asyncio
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import hashlib

load_dotenv()

class PriceTracker:
    def __init__(self):
        self.client = None
        self.db = None
        self.homepagedata_collection = None
        self.communitydata_collection = None
        self.price_history_permanent_collection = None
        self.archivedlistings_collection = None
    
    async def connect_to_mongodb(self):
        """Async MongoDB connection"""
        try:
            uri = os.getenv("MONGO_DB_URI")
            self.client = AsyncIOMotorClient(uri)
            self.db = self.client['newhomesource']
            
            self.homepagedata_collection = self.db['homepagedata']
            self.communitydata_collection = self.db['communitydata']
            self.price_history_permanent_collection = self.db['price_history_permanent']
            self.archivedlistings_collection = self.db['archivedlistings']
            
            # Create indexes for performance
            await self._create_indexes()
            
            logging.info("✅ PriceTracker connected to MongoDB")
            return self.client
        except Exception as e:
            logging.error(f"❌ PriceTracker MongoDB connection failed: {e}")
            raise Exception(f"Failed to connect to MongoDB: {e}")
    
    async def _create_indexes(self):
        """Create necessary indexes for price collections"""
        try:
            
            # Permanent price history indexes
            await self.price_history_permanent_collection.create_index([("permanent_property_id", 1)])
            await self.price_history_permanent_collection.create_index([("original_listing_id", 1)])
            await self.price_history_permanent_collection.create_index([("property_snapshot.location.county", 1)])
            
            logging.info("✅ Price tracking indexes created")
        except Exception as e:
            logging.warning(f"⚠️ Index creation warning: {e}")
    
    def generate_permanent_id(self, community_id: str) -> str:
        """Generate immutable permanent ID from community_id"""
        return hashlib.md5(community_id.encode()).hexdigest()
    
    async def capture_price_snapshots_from_stage2(self):
        """
        Capture price snapshots after Stage 2 completion
        Called from stage-2-community-extract.py
        """
        try:
            # Get today's community data
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            tomorrow_start = today_start + timedelta(days=1)
            
            today_docs = await self.communitydata_collection.find({
                "scraped_at": {"$gte": today_start.isoformat(), "$lt": tomorrow_start.isoformat()}
            }).to_list(length=None)
            
            logging.info(f"📊 Processing {len(today_docs)} community documents for price snapshots")
            
            if len(today_docs) == 0:
                logging.info("ℹ️ No community data found for today - skipping price snapshot capture")
                return
                
            price_snapshots = []
            total_communities_processed = 0
            
            for doc in today_docs:
                listing_id = doc.get("listing_id")
                if not listing_id:
                    continue
                
                communities = doc.get("community_data", {}).get("communities", [])
                total_communities_processed += len(communities)
                
                for community in communities:
                    snapshot = await self._create_price_snapshot(community, listing_id, doc)
                    if snapshot:
                        price_snapshots.append(snapshot)
            
            # Log processing summary
            logging.info(f"🏠 Analyzed {total_communities_processed} individual communities from {len(today_docs)} properties")
            
            # Update permanent storage for active properties
            if price_snapshots:
                await self._update_permanent_timelines(price_snapshots)
                logging.info(f"💾 Updated {len(price_snapshots)} permanent timeline entries")
            else:
                logging.info("ℹ️ No price changes detected - no new snapshots created")
                
        except Exception as e:
            logging.error(f"❌ Error capturing price snapshots: {e}")
    
    async def _create_price_snapshot(self, community: Dict, listing_id: str, source_doc: Dict) -> Optional[Dict]:
        """Create individual price snapshot with change detection"""
        try:
            community_id = community.get("community_id")
            current_price = float(community.get("price", 0))
            
            if not community_id or current_price <= 0:
                return None
            
            # Get previous price from permanent storage
            permanent_id = self.generate_permanent_id(community_id)
            permanent_record = await self.price_history_permanent_collection.find_one(
                {"permanent_property_id": permanent_id}
            )
            
            previous_price = 0
            if permanent_record and permanent_record.get("price_timeline"):
                last_timeline_entry = permanent_record["price_timeline"][-1]
                previous_price = last_timeline_entry.get("price", 0)
            
            # Only create snapshot if price changed or first time
            if current_price != previous_price or not permanent_record:
                change_amount = current_price - previous_price
                change_percentage = (change_amount / previous_price * 100) if previous_price > 0 else 0
                
                return {
                    "listing_id": listing_id,
                    "community_id": community_id,
                    "property_name": community.get("name", ""),
                    "price": current_price,
                    "price_currency": community.get("price_currency", "USD"),
                    "snapshot_date": datetime.now(),
                    "scraped_at": source_doc.get("scraped_at"),
                    "build_status": community.get("build_status", []),
                    "build_type": community.get("build_type", ""),
                    "change_metrics": {
                        "previous_price": previous_price,
                        "change_amount": change_amount,
                        "change_percentage": round(change_percentage, 2),
                        "is_significant": abs(change_percentage) >= 5.0
                    },
                    "data_source": "stage2_community",
                }
            
            return None
            
        except Exception as e:
            logging.debug(f"Error creating price snapshot: {e}")
            return None
    
    async def _update_permanent_timelines(self, snapshots: List[Dict]):
        """Update permanent storage with new price points"""
        try:
            for snapshot in snapshots:
                listing_id = snapshot["listing_id"]
                community_id = snapshot["community_id"]
                permanent_id = self.generate_permanent_id(community_id)
                
                # Get property metadata from homepagedata
                property_doc = await self.homepagedata_collection.find_one({"listing_id": listing_id})
                if not property_doc:
                    continue
                
                # Create timeline entry
                timeline_entry = {
                    "date": snapshot["snapshot_date"],
                    "price": snapshot["price"],
                    "currency": snapshot["price_currency"],
                    "source": "stage2",
                    "change_type": self._classify_change_type(snapshot["change_metrics"]),
                    "context": {
                        "build_status": snapshot.get("build_status", []),
                        "build_type": snapshot.get("build_type", ""),
                        "change_percentage": snapshot["change_metrics"]["change_percentage"]
                    }
                }
                
                # Build community snapshot
                community_snapshot = self._build_community_snapshot(target_community, listing_id)
                
                # Calculate aggregated metrics
                aggregated_metrics = await self._calculate_aggregated_metrics(permanent_id, snapshot["price"])
                
                # Upsert permanent record
                await self.price_history_permanent_collection.update_one(
                    {"permanent_property_id": permanent_id},
                    {
                        "$set": {
                            **community_snapshot,
                            "aggregated_metrics": aggregated_metrics,
                            "last_updated": datetime.now()
                        },
                        "$push": {"price_timeline": timeline_entry},
                        "$setOnInsert": {"created_at": datetime.now()}
                    },
                    upsert=True
                )
            
            logging.info(f"✅ Updated {len(snapshots)} permanent timeline entries")
            
        except Exception as e:
            logging.error(f"❌ Error updating permanent timelines: {e}")
    
    def _classify_change_type(self, change_metrics: Dict) -> str:
        """Classify the type of price change"""
        change_amount = change_metrics.get("change_amount", 0)
        
        if change_amount == 0:
            return "unchanged"
        elif change_amount > 0:
            return "increase"
        else:
            return "decrease"
    
    def _build_community_snapshot(self, community: Dict, listing_id: str) -> Dict:
        """Build community metadata for permanent storage"""
        address = community.get("address", {})
        
        return {
            "permanent_property_id": self.generate_permanent_id(community.get("community_id", "")),
            "community_id": community.get("community_id", ""),
            "accommodationCategory": community.get("accommodationCategory", ""),
            "offeredBy": community.get("offeredBy", ""),
            "community_name": community.get("name", ""),
            "listing_status": "active",
            "address": {
                "county": address.get("county", ""),
                "addressLocality": address.get("addressLocality", ""),
                "addressRegion": address.get("addressRegion", ""),
                "streetAddress": address.get("streetAddress", ""),
                "postalCode": address.get("postalCode", "")
            }
        }
    
    def _extract_county_from_address(self, address: Dict) -> str:
        """Extract county from address data"""
        # This would need to be customized based on your location mapping
        city = address.get("addressLocality", "").lower()
        if "ventura" in city:
            return "Ventura County"
        elif "riverside" in city or "temecula" in city:
            return "Riverside County"
        return "Unknown County"
    
    async def consolidate_to_permanent_storage(self, listing_id: str):
        """
        Consolidate price history to permanent storage when property is archived
        Called from Stage 1 archiving process
        """
        try:
            logging.info(f"🔄 Consolidating price history for {listing_id}")
            
            # Get permanent price history for this property
            permanent_records = await self.price_history_permanent_collection.find(
                {"original_listing_id": listing_id}
            ).to_list(length=None)
            
            if not permanent_records:
                logging.warning(f"⚠️ No permanent price history found for {listing_id}")
                return
            
            # Get archived property metadata
            archived_property = await self.archivedlistings_collection.find_one(
                {"listing_id": listing_id}
            )
            
            if not archived_property:
                logging.error(f"❌ No archived property found for {listing_id}")
                return
            
            # Update permanent storage to mark as archived
            for record in permanent_records:
                await self.price_history_permanent_collection.update_one(
                    {"permanent_property_id": record["permanent_property_id"]},
                    {"$set": {
                        "property_snapshot.status": "archived",
                        "archive_metadata": {
                            "archived_at": datetime.now(),
                            "archive_reason": archived_property.get("archive_reason", "unknown")
                        },
                        "last_updated": datetime.now()
                    }}
                )
            
            logging.info(f"✅ Marked {len(permanent_records)} permanent records as archived for {listing_id}")
            
        except Exception as e:
            logging.error(f"❌ Error consolidating price history for {listing_id}: {e}")
    
    def _calculate_volatility(self, prices: List[float]) -> float:
        """Calculate simple price volatility score"""
        if len(prices) < 2:
            return 0.0
        
        changes = []
        for i in range(1, len(prices)):
            if prices[i-1] > 0:
                change_pct = abs((prices[i] - prices[i-1]) / prices[i-1] * 100)
                changes.append(change_pct)
        
        return sum(changes) / len(changes) if changes else 0.0
    
    async def archive_community_data(self, listing_id: str):
        """Archive community data when parent listing is archived"""
        try:
            result = await self.communitydata_collection.update_one(
                {"listing_id": listing_id},
                {"$set": {
                    "listing_status": "archived",
                    "archived_at": datetime.now(),
                    "archive_reason": "parent_listing_archived"
                }}
            )
            
            if result.modified_count > 0:
                logging.info(f"✅ Archived community data for {listing_id}")
            else:
                logging.warning(f"⚠️ No community data found to archive for {listing_id}")
                
        except Exception as e:
            logging.error(f"❌ Error archiving community data for {listing_id}: {e}")
    
    async def cleanup_old_price_history(self, days_to_keep: int = 365):
        """Clean up old price history records (keep permanent storage)"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            # Clean up old archived permanent records (optional - could keep for historical analysis)
            result = await self.price_history_permanent_collection.delete_many({
                "archive_metadata.archived_at": {"$lt": cutoff_date}
            })
            
            logging.info(f"🧹 Cleaned up {result.deleted_count} old archived permanent records")
            
        except Exception as e:
            logging.error(f"❌ Error cleaning up price history: {e}")
    
    def close_connection(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logging.info("🔌 PriceTracker MongoDB connection closed")

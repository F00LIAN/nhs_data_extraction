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
        self.pricehistory_collection = None
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
            self.pricehistory_collection = self.db['pricehistory']
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
            # Price history indexes
            await self.pricehistory_collection.create_index([("listing_id", 1), ("snapshot_date", -1)])
            await self.pricehistory_collection.create_index([("community_id", 1), ("snapshot_date", -1)])
            await self.pricehistory_collection.create_index([("is_archived", 1)])
            
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
            
            # Bulk insert snapshots
            if price_snapshots:
                await self.pricehistory_collection.insert_many(price_snapshots)
                logging.info(f"✅ Created {len(price_snapshots)} new price snapshots")
                
                # Also update permanent storage for active properties
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
            
            # Check for previous price
            last_snapshot = await self.pricehistory_collection.find_one(
                {"community_id": community_id},
                sort=[("snapshot_date", -1)]
            )
            
            previous_price = last_snapshot.get("price", 0) if last_snapshot else 0
            
            # Only create snapshot if price changed or first time
            if current_price != previous_price or not last_snapshot:
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
                    "is_archived": False
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
                
                # Upsert permanent record
                await self.price_history_permanent_collection.update_one(
                    {"permanent_property_id": permanent_id},
                    {
                        "$set": {
                            "original_listing_id": listing_id,
                            "community_id": community_id,
                            "community_name": snapshot["property_name"],
                            "property_snapshot": self._build_property_snapshot(property_doc),
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
    
    def _build_property_snapshot(self, property_doc: Dict) -> Dict:
        """Build frozen property metadata for permanent storage"""
        property_data = property_doc.get("property_data", {})
        address = property_data.get("address", {})
        geo = property_data.get("geo", {})
        
        return {
            "name": property_data.get("name", ""),
            "developer": property_data.get("offers", {}).get("offeredBy", ""),
            "location": {
                "county": self._extract_county_from_address(address),
                "city": address.get("addressLocality", ""),
                "coordinates": {
                    "latitude": geo.get("latitude"),
                    "longitude": geo.get("longitude")
                },
                "address": f"{address.get('streetAddress', '')}, {address.get('addressLocality', '')}, {address.get('addressRegion', '')}"
            },
            "first_seen": property_doc.get("first_scraped_at", property_doc.get("scraped_at")),
            "last_seen": property_doc.get("scraped_at"),
            "status": "active"
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
            
            # Get all price history for this property (includes all communities)
            price_records = await self.pricehistory_collection.find(
                {"listing_id": listing_id}
            ).sort("snapshot_date", 1).to_list(length=None)
            
            if not price_records:
                logging.warning(f"⚠️ No price history found for {listing_id}")
                return
            
            # Get archived property metadata
            archived_property = await self.archivedlistings_collection.find_one(
                {"listing_id": listing_id}
            )
            
            if not archived_property:
                logging.error(f"❌ No archived property found for {listing_id}")
                return
            
            # Group price records by community_id
            communities_data = {}
            for record in price_records:
                community_id = record.get("community_id")
                if community_id:
                    if community_id not in communities_data:
                        communities_data[community_id] = []
                    communities_data[community_id].append(record)
            
            # Process each community separately
            consolidated_count = 0
            for community_id, community_records in communities_data.items():
                permanent_id = self.generate_permanent_id(community_id)
                
                # Build complete timeline for this community
                price_timeline = []
                for record in community_records:
                    price_timeline.append({
                        "date": record["snapshot_date"],
                        "price": record["price"],
                        "currency": record.get("price_currency", "USD"),
                        "source": record.get("data_source", "unknown"),
                        "change_type": self._classify_change_type(record.get("change_metrics", {})),
                        "context": {
                            "build_status": record.get("build_status", []),
                            "build_type": record.get("build_type", ""),
                            "change_percentage": record.get("change_metrics", {}).get("change_percentage", 0)
                        }
                    })
                
                # Calculate aggregated metrics for this community
                prices = [entry["price"] for entry in price_timeline]
                metrics = {
                    "min_price": min(prices),
                    "max_price": max(prices),
                    "avg_price": sum(prices) / len(prices),
                    "total_days_tracked": (price_timeline[-1]["date"] - price_timeline[0]["date"]).days if len(price_timeline) > 1 else 0,
                    "total_price_changes": len(price_timeline),
                    "volatility_score": self._calculate_volatility(prices)
                }
                
                # Update permanent storage with complete data for this community
                await self.price_history_permanent_collection.update_one(
                    {"permanent_property_id": permanent_id},
                    {"$set": {
                        "original_listing_id": listing_id,
                        "community_id": community_id,
                        "community_name": community_records[0].get("property_name", ""),
                        "property_snapshot": {
                            **self._build_property_snapshot(archived_property),
                            "status": "archived"
                        },
                        "price_timeline": price_timeline,
                        "aggregated_metrics": metrics,
                        "archive_metadata": {
                            "archived_at": datetime.now(),
                            "archive_reason": archived_property.get("archive_reason", "unknown"),
                            "final_price": price_timeline[-1]["price"] if price_timeline else None,
                            "total_price_changes": len(price_timeline)
                        },
                        "last_updated": datetime.now()
                    }},
                    upsert=True
                )
                consolidated_count += 1
            
            # Mark price history as archived
            await self.pricehistory_collection.update_many(
                {"listing_id": listing_id},
                {"$set": {"is_archived": True, "archived_at": datetime.now()}}
            )
            
            logging.info(f"✅ Consolidated {consolidated_count} communities with {len(price_records)} total price records for {listing_id}")
            
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
                    "is_archived": True,
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
            
            result = await self.pricehistory_collection.delete_many({
                "snapshot_date": {"$lt": cutoff_date},
                "is_archived": True
            })
            
            logging.info(f"🧹 Cleaned up {result.deleted_count} old price history records")
            
        except Exception as e:
            logging.error(f"❌ Error cleaning up price history: {e}")
    
    def close_connection(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logging.info("🔌 PriceTracker MongoDB connection closed")

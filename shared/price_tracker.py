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
        self.price_city_snapshot_collection = None
    
    async def connect_to_mongodb(self):
        """Async MongoDB connection"""
        try:
            uri = os.getenv("MONGO_DB_URI")
            self.client = AsyncIOMotorClient(uri)
            self.db = self.client['newhomesource']
            
            self.homepagedata_collection = self.db['homepagedata']
            self.communitydata_collection = self.db['communitydata']
            self.price_history_permanent_collection = self.db['price_history_permanent']
            self.price_city_snapshot_collection = self.db['price_city_snapshot']
            
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
            
            # City price snapshot indexes
            await self.price_city_snapshot_collection.create_index([("city_id", 1)])
            await self.price_city_snapshot_collection.create_index([("addressLocality", 1)])
            await self.price_city_snapshot_collection.create_index([("county", 1)])
            
            logging.info("✅ Price tracking indexes created")
        except Exception as e:
            logging.warning(f"⚠️ Index creation warning: {e}")
    
    def generate_permanent_id(self, community_id: str) -> str:
        """Generate immutable permanent ID from community_id"""
        return hashlib.md5(community_id.encode()).hexdigest()
    
    async def capture_price_snapshots_from_stage2(self):
        """
        Capture daily price snapshots for all active communities
        Called from Stage 2 completion
        """
        try:
            # Get all active community data (not just today's)
            active_docs = await self.communitydata_collection.find({
                "listing_status": {"$in": ["active", "new", "updated"]}
            }).to_list(length=None)
            
            
            logging.info(f"📊 Processing {len(active_docs)} active community documents for daily price snapshots")
            
            if len(active_docs) == 0:
                logging.info("ℹ️ No active community data found - skipping price snapshot capture")
                return
                
            price_snapshots = []
            total_communities_processed = 0
            
            for doc in active_docs:
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
            logging.info(f"🏠 Analyzed {total_communities_processed} individual communities from {len(active_docs)} properties")
            logging.info(f"📈 Created {len(price_snapshots)} price snapshots for permanent storage")
            
            # Update permanent storage for active properties
            if price_snapshots:
                await self._update_permanent_timelines(price_snapshots)
                logging.info(f"✅ Successfully processed {len(price_snapshots)} price snapshots")
                
                # Create city price snapshots after updating permanent storage
                await self._create_city_price_snapshots()
                logging.info("✅ City price snapshots updated")
            else:
                logging.warning("⚠️ No valid price snapshots created")
                
        except Exception as e:
            logging.error(f"❌ Error capturing price snapshots: {e}")
    
    async def _create_price_snapshot(self, community: Dict, listing_id: str, source_doc: Dict) -> Optional[Dict]:
        """Create individual price snapshot with change detection"""
        try:
            community_name = community.get('name', 'Unknown')
            logging.debug(f"🔍 Creating price snapshot for community: {community_name}")
            community_id = community.get("community_id")
            current_price = float(community.get("price", 0))
            
            if not community_id or current_price <= 0:
                logging.debug(f"⚠️ Skipping {community_name}: community_id={community_id}, price={current_price}")
                return None
            
            logging.debug(f"✅ Valid community for price tracking: {community_name} @ ${current_price}")
            
            # Get previous price from permanent storage
            permanent_id = self.generate_permanent_id(community_id)
            permanent_record = await self.price_history_permanent_collection.find_one(
                {"permanent_property_id": permanent_id}
            )
            
            previous_price = 0
            if permanent_record and permanent_record.get("price_timeline"):
                last_timeline_entry = permanent_record["price_timeline"][-1]
                previous_price = last_timeline_entry.get("price", 0)
            
            # Always create daily snapshot regardless of price change
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
            
        except Exception as e:
            logging.debug(f"Error creating price snapshot: {e}")
            return None
    
    async def _update_permanent_timelines(self, snapshots: List[Dict]):
        """Update permanent storage with new price points"""
        try:
            logging.info(f"🔄 Starting permanent timeline updates for {len(snapshots)} snapshots")
            updated_count = 0
            for snapshot in snapshots:
                listing_id = snapshot["listing_id"]
                community_id = snapshot["community_id"]
                permanent_id = self.generate_permanent_id(community_id)
                
                # Get community data from communitydata
                community_doc = await self.communitydata_collection.find_one({"listing_id": listing_id})
                if not community_doc:
                    logging.warning(f"⚠️ No community data found for listing_id: {listing_id}")
                    continue
                
                # Find the specific community within the document
                communities = community_doc.get("community_data", {}).get("communities", [])
                target_community = next((c for c in communities if c.get("community_id") == community_id), None)
                if not target_community:
                    logging.warning(f"⚠️ Community {community_id} not found in listing {listing_id}")
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
                result = await self.price_history_permanent_collection.update_one(
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
            
                if result.upserted_id:
                    logging.info(f"🆕 Created new price_history_permanent record for {community_id}")
                elif result.modified_count > 0:
                    logging.info(f"🔄 Updated price_history_permanent record for {community_id}")
                else:
                    logging.warning(f"⚠️ No changes made to price_history_permanent for {community_id}")
                
                updated_count += 1
            
            logging.info(f"✅ Processed {updated_count}/{len(snapshots)} permanent timeline entries")
            
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
        
        # Log address data availability for debugging
        if address:
            address_keys = list(address.keys())
            logging.info(f"🏠 Building price history snapshot for '{community.get('name')}' with address keys: {address_keys}")
        else:
            logging.warning(f"⚠️ No address data found for community '{community.get('name')}' in price history snapshot")
        
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
    
    async def _calculate_aggregated_metrics(self, permanent_id: str, current_price: float) -> Dict:
        """Calculate aggregated metrics for permanent storage"""
        try:
            # Get existing price timeline
            existing_record = await self.price_history_permanent_collection.find_one(
                {"permanent_property_id": permanent_id}
            )
            
            price_timeline = existing_record.get("price_timeline", []) if existing_record else []
            all_prices = [entry.get("price", 0) for entry in price_timeline] + [current_price]
            all_prices = [p for p in all_prices if p > 0]  # Filter out invalid prices
            
            if not all_prices:
                return {}
            
            # Calculate basic metrics
            most_recent_price = all_prices[-1]
            average_price = sum(all_prices) / len(all_prices)
            min_price = min(all_prices)
            max_price = max(all_prices)
            total_days_tracked = len(all_prices)
            
            # Calculate moving averages (simplified for now)
            moving_averages = {
                "1_day_average": most_recent_price,
                "7_day_average": average_price,
                "30_day_average": average_price,
                "90_day_average": average_price,
                "180_day_average": average_price,
                "365_day_average": average_price
            }
            
            # Calculate percent changes (simplified for now)
            percent_change_metrics = {
                "1_day_change": 0,
                "7_day_change": 0,
                "30_day_change": 0,
                "90_day_change": 0,
                "180_day_change": 0,
                "365_day_change": 0
            }
            
            return {
                "most_recent_price": most_recent_price,
                "average_price": round(average_price, 2),
                "min_price": min_price,
                "max_price": max_price,
                "total_days_tracked": total_days_tracked,
                "moving_averages": moving_averages,
                "percent_change_metrics": percent_change_metrics
            }
            
        except Exception as e:
            logging.error(f"❌ Error calculating aggregated metrics: {e}")
            return {}
    
    async def _create_city_price_snapshots(self):
        """Create aggregated city price snapshots from permanent storage"""
        try:
            logging.info("🏙️ Creating city price snapshots...")
            
            # Get all properties (active + archived) grouped by city
            pipeline = [
                {
                    "$group": {
                        "_id": {
                            "city": "$address.addressLocality",
                            "county": "$address.county",
                            "addressRegion": "$address.addressRegion"
                        },
                        "properties": {
                            "$push": {
                                "accommodation_category": "$accommodationCategory",
                                "listing_status": "$listing_status",
                                "current_price": "$aggregated_metrics.most_recent_price",
                                "price_timeline": "$price_timeline"
                            }
                        }
                    }
                }
            ]
            
            city_aggregates = await self.price_history_permanent_collection.aggregate(pipeline).to_list(length=None)
            
            for city_data in city_aggregates:
                city_info = city_data["_id"]
                city_id = self.generate_permanent_id(f"{city_info['city']}_{city_info['county']}_{city_info['addressRegion']}")
                
                # Process properties by type and status
                properties = city_data["properties"]
                
                # Active properties only
                active_properties = [p for p in properties if p["listing_status"] == "active"]
                active_sfr = [p for p in active_properties if p["accommodation_category"] == "Single Family Residence"]
                active_condo = [p for p in active_properties if p["accommodation_category"] == "Condominium"]
                
                # Calculate current active metrics
                sfr_count = len(active_sfr)
                condo_count = len(active_condo)
                sfr_avg_price = sum(p["current_price"] for p in active_sfr) / len(active_sfr) if active_sfr else None
                condo_avg_price = sum(p["current_price"] for p in active_condo) / len(active_condo) if active_condo else None
                overall_avg_price = sum(p["current_price"] for p in active_properties) / len(active_properties) if active_properties else None
                
                # Get existing historical data to preserve listing counts
                existing_snapshot = await self.price_city_snapshot_collection.find_one({"city_id": city_id})
                existing_historical = existing_snapshot.get("historical_daily_averages", []) if existing_snapshot else []
                
                # Calculate historical daily averages from all properties (active + archived)
                calculated_historical = await self._calculate_historical_daily_averages(properties)
                
                # Preserve historical listing counts while updating prices
                historical_daily_averages = await self._preserve_historical_listing_counts(
                    existing_historical, calculated_historical, city_info
                )
                
                # Add or update today's entry with current active metrics
                today = datetime.now().date().isoformat()
                today_entry = {
                    "date": today,
                    "sfr_avg_price": sfr_avg_price,
                    "sfr_listing_count": sfr_count,  # Current active SFR count
                    "condo_avg_price": condo_avg_price,
                    "condo_listing_count": condo_count,  # Current active condo count
                    "overall_avg_price": overall_avg_price,
                    "overall_listing_count": len(active_properties)  # Current total active count
                }
                
                # Update or append today's entry
                today_exists = False
                for i, entry in enumerate(historical_daily_averages):
                    if entry.get("date") == today:
                        historical_daily_averages[i] = today_entry
                        today_exists = True
                        break
                
                if not today_exists:
                    historical_daily_averages.append(today_entry)
                
                # Sort by date and keep last 30 days
                historical_daily_averages.sort(key=lambda x: x["date"])
                historical_daily_averages = historical_daily_averages[-30:]
                
                # Calculate moving averages and percent changes
                sfr_metrics = await self._calculate_property_metrics(active_sfr, "sfr")
                condo_metrics = await self._calculate_property_metrics(active_condo, "condo")
                overall_metrics = await self._calculate_property_metrics(active_properties, "overall")
                
                # Build city snapshot document
                city_snapshot = {
                    "city_id": city_id,
                    "addressLocality": city_info["city"],
                    "county": city_info["county"],
                    "addressRegion": city_info["addressRegion"],
                    "current_active_metrics": {
                        "sfr": {
                            "count": sfr_count,
                            "avg_price": sfr_avg_price,
                            "moving_averages": sfr_metrics["moving_averages"],
                            "percent_changes": sfr_metrics["percent_changes"]
                        },
                        "condo": {
                            "count": condo_count,
                            "avg_price": condo_avg_price,
                            "moving_averages": condo_metrics["moving_averages"],
                            "percent_changes": condo_metrics["percent_changes"]
                        },
                        "overall": {
                            "total_properties": len(active_properties),
                            "avg_price": overall_avg_price,
                            "moving_averages": overall_metrics["moving_averages"],
                            "percent_changes": overall_metrics["percent_changes"]
                        }
                    },
                    "historical_daily_averages": historical_daily_averages,
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
            logging.error(f"❌ Error creating city price snapshots: {e}")
    
    async def _preserve_historical_listing_counts(self, existing_historical: List[Dict], 
                                                calculated_historical: List[Dict], 
                                                city_info: Dict) -> List[Dict]:
        """
        Preserve actual listing counts from when historical snapshots were taken.
        Only update prices, not counts for historical dates.
        """
        try:
            # Create a lookup of existing counts by date
            existing_counts_by_date = {}
            for entry in existing_historical:
                date = entry.get("date")
                if date:
                    existing_counts_by_date[date] = {
                        "sfr_listing_count": entry.get("sfr_listing_count"),
                        "condo_listing_count": entry.get("condo_listing_count"), 
                        "overall_listing_count": entry.get("overall_listing_count")
                    }
            
            # Merge calculated historical (prices) with preserved counts
            preserved_historical = []
            for calc_entry in calculated_historical:
                date = calc_entry.get("date")
                
                # If we have existing counts for this date, preserve them
                if date in existing_counts_by_date:
                    preserved_entry = calc_entry.copy()
                    # Preserve the original listing counts from when snapshot was taken
                    preserved_counts = existing_counts_by_date[date]
                    if preserved_counts["sfr_listing_count"] is not None:
                        preserved_entry["sfr_listing_count"] = preserved_counts["sfr_listing_count"]
                    if preserved_counts["condo_listing_count"] is not None:
                        preserved_entry["condo_listing_count"] = preserved_counts["condo_listing_count"]
                    if preserved_counts["overall_listing_count"] is not None:
                        preserved_entry["overall_listing_count"] = preserved_counts["overall_listing_count"]
                    
                    preserved_historical.append(preserved_entry)
                    logging.debug(f"📊 Preserved counts for {date}: SFR={preserved_counts['sfr_listing_count']}, Overall={preserved_counts['overall_listing_count']}")
                else:
                    # New historical date - use calculated values
                    preserved_historical.append(calc_entry)
                    logging.debug(f"📊 New historical date {date}: SFR={calc_entry.get('sfr_listing_count')}, Overall={calc_entry.get('overall_listing_count')}")
            
            logging.info(f"🔄 Preserved historical counts for {len(existing_counts_by_date)} existing dates in {city_info.get('city', 'Unknown City')}")
            return preserved_historical
            
        except Exception as e:
            logging.error(f"❌ Error preserving historical listing counts: {e}")
            # Fallback to calculated historical if preservation fails
            return calculated_historical
    
    async def _calculate_historical_daily_averages(self, properties: List[Dict]) -> List[Dict]:
        """Calculate daily average prices and active listing counts from all properties' timelines"""
        try:
            daily_data = {}
            
            # First, determine all unique dates across all timelines
            all_dates = set()
            for prop in properties:
                timeline = prop.get("price_timeline", [])
                for entry in timeline:
                    date = entry.get("date")
                    if date:
                        if hasattr(date, 'date'):
                            date_str = date.date().isoformat()
                        elif isinstance(date, str):
                            date_str = date[:10]
                        else:
                            continue
                        all_dates.add(date_str)
            
            # For each date, find all properties that were active (had a price entry on or before that date)
            for date_str in all_dates:
                target_date = datetime.fromisoformat(date_str).date()
                
                daily_data[date_str] = {
                    "sfr": {"prices": [], "properties": set()}, 
                    "condo": {"prices": [], "properties": set()}, 
                    "all": {"prices": [], "properties": set()}
                }
                
                for prop in properties:
                    timeline = prop.get("price_timeline", [])
                    prop_type = prop.get("accommodation_category", "")
                    prop_id = prop.get("permanent_property_id") or prop.get("community_id", "")
                    
                    # Find the most recent price entry for this property on or before target_date
                    valid_entries = []
                    for entry in timeline:
                        entry_date = entry.get("date")
                        if entry_date:
                            if hasattr(entry_date, 'date'):
                                entry_date_obj = entry_date.date()
                            elif isinstance(entry_date, str):
                                try:
                                    entry_date_obj = datetime.fromisoformat(entry_date[:10]).date()
                                except:
                                    continue
                            else:
                                continue
                            
                            # Only include entries on or before target date
                            if entry_date_obj <= target_date:
                                valid_entries.append((entry_date_obj, entry.get("price")))
                    
                    # If property has any valid entries, it was active on this date
                    if valid_entries:
                        # Get the most recent price (latest date)
                        valid_entries.sort(key=lambda x: x[0], reverse=True)
                        latest_price = valid_entries[0][1]
                        
                        if latest_price:
                            # Add to daily data
                            daily_data[date_str]["all"]["prices"].append(latest_price)
                            daily_data[date_str]["all"]["properties"].add(prop_id)
                            
                            if prop_type == "Single Family Residence":
                                daily_data[date_str]["sfr"]["prices"].append(latest_price)
                                daily_data[date_str]["sfr"]["properties"].add(prop_id)
                            elif prop_type == "Condominium":
                                daily_data[date_str]["condo"]["prices"].append(latest_price)
                                daily_data[date_str]["condo"]["properties"].add(prop_id)
            
            # Calculate daily averages with accurate listing counts
            daily_averages = []
            for date_str in sorted(daily_data.keys()):
                day_data = daily_data[date_str]
                
                sfr_prices = day_data["sfr"]["prices"]
                condo_prices = day_data["condo"]["prices"]
                all_prices = day_data["all"]["prices"]
                
                daily_averages.append({
                    "date": date_str,
                    "sfr_avg_price": round(sum(sfr_prices) / len(sfr_prices), 2) if sfr_prices else None,
                    "sfr_listing_count": len(day_data["sfr"]["properties"]),
                    "condo_avg_price": round(sum(condo_prices) / len(condo_prices), 2) if condo_prices else None,
                    "condo_listing_count": len(day_data["condo"]["properties"]),
                    "overall_avg_price": round(sum(all_prices) / len(all_prices), 2) if all_prices else None,
                    "overall_listing_count": len(day_data["all"]["properties"])
                })
            
            return daily_averages[-30:]  # Return last 30 days
            
        except Exception as e:
            logging.error(f"❌ Error calculating historical daily averages: {e}")
            return []
    
    async def _calculate_property_metrics(self, properties: List[Dict], property_type: str) -> Dict:
        """Calculate moving averages and percent changes for property type"""
        try:
            if not properties:
                return {
                    "moving_averages": {
                        "7_day_average": None,
                        "30_day_average": None,
                        "90_day_average": None
                    },
                    "percent_changes": {
                        "1_day_change": None,
                        "7_day_change": None,
                        "30_day_change": None,
                        "90_day_change": None
                    }
                }
            
            # Collect all timeline prices for this property type
            all_prices = []
            for prop in properties:
                timeline = prop.get("price_timeline", [])
                for entry in timeline:
                    date = entry.get("date")
                    price = entry.get("price")
                    if date and price:
                        if hasattr(date, 'date'):
                            date_obj = date
                        else:
                            date_obj = datetime.fromisoformat(str(date).replace('Z', '+00:00'))
                        all_prices.append((date_obj, price))
            
            # Sort by date
            all_prices.sort(key=lambda x: x[0])
            
            if not all_prices:
                return {
                    "moving_averages": {"7_day_average": None, "30_day_average": None, "90_day_average": None},
                    "percent_changes": {"1_day_change": None, "7_day_change": None, "30_day_change": None, "90_day_change": None}
                }
            
            # Calculate moving averages
            current_date = datetime.now()
            moving_averages = {}
            percent_changes = {}
            
            for days in [7, 30, 90]:
                cutoff_date = current_date - timedelta(days=days)
                recent_prices = [price for date, price in all_prices if date >= cutoff_date]
                
                if recent_prices:
                    avg = sum(recent_prices) / len(recent_prices)
                    moving_averages[f"{days}_day_average"] = round(avg, 2)
                else:
                    moving_averages[f"{days}_day_average"] = None
            
            # Calculate percent changes
            current_avg = sum(p["current_price"] for p in properties) / len(properties)
            
            for days in [1, 7, 30, 90]:
                cutoff_date = current_date - timedelta(days=days)
                past_prices = [price for date, price in all_prices if date >= cutoff_date - timedelta(days=1) and date < cutoff_date]
                
                if past_prices:
                    past_avg = sum(past_prices) / len(past_prices)
                    change = ((current_avg - past_avg) / past_avg * 100) if past_avg > 0 else 0
                    percent_changes[f"{days}_day_change"] = round(change, 2)
                else:
                    percent_changes[f"{days}_day_change"] = None
            
            return {
                "moving_averages": moving_averages,
                "percent_changes": percent_changes
            }
            
        except Exception as e:
            logging.error(f"❌ Error calculating property metrics for {property_type}: {e}")
            return {
                "moving_averages": {"7_day_average": None, "30_day_average": None, "90_day_average": None},
                "percent_changes": {"1_day_change": None, "7_day_change": None, "30_day_change": None, "90_day_change": None}
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
            
            # Note: Legacy archival lookup removed - price history preserved permanently
            
            # Update permanent storage to mark as archived
            for record in permanent_records:
                await self.price_history_permanent_collection.update_one(
                    {"permanent_property_id": record["permanent_property_id"]},
                    {"$set": {
                        "property_snapshot.status": "archived",
                        "archive_metadata": {
                            "archived_at": datetime.now(),
                            "archive_reason": "property archived"
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

    async def update_archived_community_status(self, listing_id: str):
        """Update listing_status to archived in price_history_permanent when community is archived"""
        try:
            # Get the archived community data to find individual community_ids
            archived_doc = await self.db['communitydata_archived'].find_one({"listing_id": listing_id})
            
            if not archived_doc:
                logging.warning(f"⚠️ No archived community data found for {listing_id}")
                return
            
            communities = archived_doc.get("community_data", {}).get("communities", [])
            updated_count = 0
            
            for community in communities:
                community_id = community.get("community_id")
                if community_id:
                    permanent_id = self.generate_permanent_id(community_id)
                    
                    # Update listing_status to archived in price_history_permanent
                    result = await self.price_history_permanent_collection.update_one(
                        {"permanent_property_id": permanent_id},
                        {"$set": {
                            "listing_status": "archived",
                            "archived_at": datetime.now(),
                            "last_updated": datetime.now()
                        }}
                    )
                    
                    if result.modified_count > 0:
                        updated_count += 1
                        logging.info(f"✅ Updated price history status to archived for {community_id}")
            
            logging.info(f"✅ Updated {updated_count} price history records to archived status for {listing_id}")
            
        except Exception as e:
            logging.error(f"❌ Error updating archived community status for {listing_id}: {e}")
    
    async def cleanup_old_price_history(self, days_to_keep: int = 365):
        """Clean up old price history records (keep permanent storage)"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            # Note: Price history is kept permanently, no cleanup needed
            logging.info("ℹ️ Price history cleanup skipped - permanent storage maintained")
            
        except Exception as e:
            logging.error(f"❌ Error in cleanup_old_price_history: {e}")
    
    def close_connection(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logging.info("🔌 PriceTracker MongoDB connection closed")

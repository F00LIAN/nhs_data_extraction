#!/usr/bin/env python3
"""
Temporary script to update price_city_snapshot collection with enhanced time-based views
Usage: python update_city_snapshots_enhanced.py
"""

import asyncio
import os
import logging
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import hashlib

load_dotenv()

class EnhancedCitySnapshotUpdater:
    def __init__(self):
        self.client = None
        self.db = None
        
    async def connect(self):
        """Connect to MongoDB"""
        uri = os.getenv("MONGO_DB_URI")
        self.client = AsyncIOMotorClient(uri)
        self.db = self.client['newhomesource']
        logging.info("✅ Connected to MongoDB")
        
    async def update_all_city_snapshots(self):
        """Update all city snapshots with enhanced time-based metrics"""
        try:
            # Get all properties grouped by city
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
                                "price_timeline": "$price_timeline"
                            }
                        }
                    }
                }
            ]
            
            city_aggregates = await self.db['price_history_permanent'].aggregate(pipeline).to_list(length=None)
            logging.info(f"📊 Processing {len(city_aggregates)} cities")
            
            for city_data in city_aggregates:
                await self._update_city_snapshot(city_data)
                
            logging.info("✅ All city snapshots updated")
            
        except Exception as e:
            logging.error(f"❌ Error updating city snapshots: {e}")
    
    async def _update_city_snapshot(self, city_data):
        """Update individual city snapshot with enhanced metrics"""
        try:
            city_info = city_data["_id"]
            city_id = hashlib.md5(f"{city_info['city']}_{city_info['county']}".encode()).hexdigest()
            properties = city_data["properties"]
            
            # Filter active properties
            active_properties = [p for p in properties if p["listing_status"] == "active"]
            active_sfr = [p for p in active_properties if p["accommodation_category"] == "Single Family Residence"]
            active_condo = [p for p in active_properties if p["accommodation_category"] == "Condominium"]
            
            # Calculate enhanced metrics
            sfr_metrics = await self._calculate_enhanced_metrics(active_sfr)
            condo_metrics = await self._calculate_enhanced_metrics(active_condo)
            overall_metrics = await self._calculate_enhanced_metrics(active_properties)
            
            # Build enhanced snapshot
            city_snapshot = {
                "city_id": city_id,
                "addressLocality": city_info["city"],
                "county": city_info["county"],
                "postalCode": city_info["postal_code"],
                "current_active_metrics": {
                    "sfr": {
                        "count": len(active_sfr),
                        "avg_price": sum(p["current_price"] for p in active_sfr) / len(active_sfr) if active_sfr else None,
                        **sfr_metrics
                    },
                    "condo": {
                        "count": len(active_condo),
                        "avg_price": sum(p["current_price"] for p in active_condo) / len(active_condo) if active_condo else None,
                        **condo_metrics
                    },
                    "overall": {
                        "total_properties": len(active_properties),
                        "avg_price": sum(p["current_price"] for p in active_properties) / len(active_properties) if active_properties else None,
                        **overall_metrics
                    }
                },
                "historical_daily_averages": await self._calculate_daily_averages(properties),
                "last_snapshot_date": datetime.now(),
                "updated_with_enhanced_views": True,
                "version": "2.0"
            }
            
            # Upsert city snapshot
            await self.db['price_city_snapshot'].update_one(
                {"city_id": city_id},
                {"$set": city_snapshot},
                upsert=True
            )
            
            logging.info(f"✅ Updated city snapshot: {city_info['city']}, {city_info['county']}")
            
        except Exception as e:
            logging.error(f"❌ Error updating city {city_info.get('city', 'Unknown')}: {e}")
    
    async def _calculate_enhanced_metrics(self, properties):
        """Calculate enhanced time-based metrics"""
        if not properties:
            return {
                "moving_averages": self._null_averages(),
                "percent_changes": self._null_changes(),
                "weekly_trends": self._null_trends(),
                "monthly_trends": self._null_trends(),
                "yearly_trends": self._null_trends()
            }
        
        # Collect all timeline prices
        all_prices = []
        for prop in properties:
            timeline = prop.get("price_timeline", [])
            for entry in timeline:
                date = entry.get("date")
                price = entry.get("price")
                if date and price:
                    try:
                        if hasattr(date, 'date'):
                            date_obj = date
                        else:
                            date_obj = datetime.fromisoformat(str(date).replace('Z', '+00:00'))
                        all_prices.append((date_obj, price))
                    except:
                        continue
        
        all_prices.sort(key=lambda x: x[0])
        current_avg = sum(p["current_price"] for p in properties) / len(properties)
        current_date = datetime.now()
        
        return {
            "moving_averages": self._calculate_moving_averages(all_prices, current_date),
            "percent_changes": self._calculate_percent_changes(all_prices, current_avg, current_date),
            "weekly_trends": self._calculate_weekly_trends(all_prices, current_avg, current_date),
            "monthly_trends": self._calculate_monthly_trends(all_prices, current_avg, current_date),
            "yearly_trends": self._calculate_yearly_trends(all_prices, current_avg, current_date)
        }
    
    def _calculate_moving_averages(self, all_prices, current_date):
        """Calculate moving averages for multiple time periods"""
        averages = {}
        for days in [1, 7, 14, 30, 60, 90, 180, 365]:
            cutoff_date = current_date - timedelta(days=days)
            recent_prices = [price for date, price in all_prices if date >= cutoff_date]
            averages[f"{days}_day_average"] = round(sum(recent_prices) / len(recent_prices), 2) if recent_prices else None
        return averages
    
    def _calculate_percent_changes(self, all_prices, current_avg, current_date):
        """Calculate percent changes for multiple time periods"""
        changes = {}
        for days in [1, 7, 14, 30, 60, 90, 180, 365]:
            cutoff_date = current_date - timedelta(days=days)
            past_prices = [price for date, price in all_prices if cutoff_date - timedelta(days=1) <= date < cutoff_date]
            if past_prices:
                past_avg = sum(past_prices) / len(past_prices)
                change = ((current_avg - past_avg) / past_avg * 100) if past_avg > 0 else 0
                changes[f"{days}_day_change"] = round(change, 2)
            else:
                changes[f"{days}_day_change"] = None
        return changes
    
    def _calculate_weekly_trends(self, all_prices, current_avg, current_date):
        """Calculate weekly trends (1, 2, 4, 8, 12 weeks)"""
        trends = {}
        for weeks in [1, 2, 4, 8, 12]:
            cutoff_date = current_date - timedelta(weeks=weeks)
            period_prices = [price for date, price in all_prices if date >= cutoff_date]
            if period_prices:
                avg_price = sum(period_prices) / len(period_prices)
                change = ((current_avg - avg_price) / avg_price * 100) if avg_price > 0 else 0
                trends[f"{weeks}_week_change"] = round(change, 2)
            else:
                trends[f"{weeks}_week_change"] = None
        return trends
    
    def _calculate_monthly_trends(self, all_prices, current_avg, current_date):
        """Calculate monthly trends (1, 3, 6, 12 months)"""
        trends = {}
        for months in [1, 3, 6, 12]:
            cutoff_date = current_date - timedelta(days=months*30)
            period_prices = [price for date, price in all_prices if date >= cutoff_date]
            if period_prices:
                avg_price = sum(period_prices) / len(period_prices)
                change = ((current_avg - avg_price) / avg_price * 100) if avg_price > 0 else 0
                trends[f"{months}_month_change"] = round(change, 2)
            else:
                trends[f"{months}_month_change"] = None
        return trends
    
    def _calculate_yearly_trends(self, all_prices, current_avg, current_date):
        """Calculate yearly trends (1, 2, 3 years)"""
        trends = {}
        for years in [1, 2, 3]:
            cutoff_date = current_date - timedelta(days=years*365)
            period_prices = [price for date, price in all_prices if date >= cutoff_date]
            if period_prices:
                avg_price = sum(period_prices) / len(period_prices)
                change = ((current_avg - avg_price) / avg_price * 100) if avg_price > 0 else 0
                trends[f"{years}_year_change"] = round(change, 2)
            else:
                trends[f"{years}_year_change"] = None
        return trends
    
    async def _calculate_daily_averages(self, properties):
        """Calculate historical daily averages with accurate listing counts (last 30 days)"""
        daily_data = {}
        
        # First, determine all unique dates across all timelines
        all_dates = set()
        for prop in properties:
            timeline = prop.get("price_timeline", [])
            for entry in timeline:
                date = entry.get("date")
                if date:
                    try:
                        if hasattr(date, 'date'):
                            date_str = date.date().isoformat()
                        else:
                            date_str = str(date)[:10]
                        all_dates.add(date_str)
                    except:
                        continue
        
        # For each date, find all properties that were active (had a price entry on or before that date)
        for date_str in all_dates:
            try:
                target_date = datetime.fromisoformat(date_str).date()
            except:
                continue
            
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
                        try:
                            if hasattr(entry_date, 'date'):
                                entry_date_obj = entry_date.date()
                            else:
                                entry_date_obj = datetime.fromisoformat(str(entry_date)[:10]).date()
                            
                            # Only include entries on or before target date
                            if entry_date_obj <= target_date:
                                valid_entries.append((entry_date_obj, entry.get("price")))
                        except:
                            continue
                
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
        
        # Build daily averages with accurate listing counts
        daily_averages = []
        for date_str in sorted(daily_data.keys())[-30:]:  # Last 30 days
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
        
        return daily_averages
    
    def _null_averages(self):
        return {f"{d}_day_average": None for d in [1, 7, 14, 30, 60, 90, 180, 365]}
    
    def _null_changes(self):
        return {f"{d}_day_change": None for d in [1, 7, 14, 30, 60, 90, 180, 365]}
    
    def _null_trends(self):
        return {}
    
    def close(self):
        if self.client:
            self.client.close()

async def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    updater = EnhancedCitySnapshotUpdater()
    try:
        await updater.connect()
        await updater.update_all_city_snapshots()
        logging.info("🎉 City snapshot enhancement complete!")
    finally:
        updater.close()

if __name__ == "__main__":
    asyncio.run(main())

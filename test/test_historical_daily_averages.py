"""
Test Historical Daily Averages with Listing Counts
Tests that current active metrics are properly included in historical daily averages
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.price_tracker import PriceTracker


class TestHistoricalDailyAverages:
    """Test historical daily averages include current active listing counts"""
    
    def setup_method(self):
        """Setup test environment"""
        self.price_tracker = PriceTracker()
    
    def test_today_entry_creation_logic(self):
        """Test the logic for creating today's entry with current active counts"""
        
        # Test data simulating active properties
        active_sfr = [
            {"current_price": 500000},
            {"current_price": 600000}
        ]
        
        active_condo = [
            {"current_price": 300000},
            {"current_price": 350000}
        ]
        
        active_properties = active_sfr + active_condo
        
        # Calculate metrics like the real function does
        sfr_count = len(active_sfr)
        condo_count = len(active_condo)
        sfr_avg_price = sum(p["current_price"] for p in active_sfr) / len(active_sfr) if active_sfr else None
        condo_avg_price = sum(p["current_price"] for p in active_condo) / len(active_condo) if active_condo else None
        overall_avg_price = sum(p["current_price"] for p in active_properties) / len(active_properties) if active_properties else None
        
        # Simulate existing historical averages
        existing_historical_averages = [
            {
                "date": "2025-09-10",
                "sfr_avg_price": 520000,
                "sfr_listing_count": 1,
                "condo_avg_price": 280000,
                "condo_listing_count": 1,
                "overall_avg_price": 400000,
                "overall_listing_count": 2
            },
            {
                "date": "2025-09-11", 
                "sfr_avg_price": 530000,
                "sfr_listing_count": 2,
                "condo_avg_price": 290000,
                "condo_listing_count": 1,
                "overall_avg_price": 410000,
                "overall_listing_count": 3
            }
        ]
        
        # Create today's entry (simulating the logic from price_tracker.py)
        today = datetime.now().date().isoformat()
        today_entry = {
            "date": today,
            "sfr_avg_price": sfr_avg_price,
            "sfr_listing_count": sfr_count,
            "condo_avg_price": condo_avg_price,
            "condo_listing_count": condo_count,
            "overall_avg_price": overall_avg_price,
            "overall_listing_count": len(active_properties)
        }
        
        # Update or append today's entry (simulating the update logic)
        historical_daily_averages = existing_historical_averages.copy()
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
        
        # Verify the results
        assert len(historical_daily_averages) == 3, "Should have 3 entries (2 existing + 1 today)"
        
        # Find today's entry
        today_entry_result = None
        for entry in historical_daily_averages:
            if entry["date"] == today:
                today_entry_result = entry
                break
        
        assert today_entry_result is not None, "Today's entry should exist"
        
        # Verify today's entry has correct counts
        assert today_entry_result["sfr_listing_count"] == 2, f"Today's SFR count should be 2, got {today_entry_result['sfr_listing_count']}"
        assert today_entry_result["condo_listing_count"] == 2, f"Today's condo count should be 2, got {today_entry_result['condo_listing_count']}"
        assert today_entry_result["overall_listing_count"] == 4, f"Today's overall count should be 4, got {today_entry_result['overall_listing_count']}"
        
        # Verify today's entry has correct prices
        expected_sfr_avg = 550000.0  # (500000 + 600000) / 2
        expected_condo_avg = 325000.0  # (300000 + 350000) / 2
        expected_overall_avg = 437500.0  # (500000 + 600000 + 300000 + 350000) / 4
        
        assert today_entry_result["sfr_avg_price"] == expected_sfr_avg, f"Today's SFR avg should be {expected_sfr_avg}"
        assert today_entry_result["condo_avg_price"] == expected_condo_avg, f"Today's condo avg should be {expected_condo_avg}"
        assert today_entry_result["overall_avg_price"] == expected_overall_avg, f"Today's overall avg should be {expected_overall_avg}"
    
    @pytest.mark.asyncio
    async def test_no_condos_scenario(self):
        """Test scenario with no condos - should have 0 counts and null prices"""
        
        mock_aggregation_result = [
            {
                "_id": {
                    "city": "NoCondoCity",
                    "county": "Test County",
                    "postal_code": "54321"
                },
                "properties": [
                    {
                        "accommodation_category": "Single Family Residence",
                        "listing_status": "active",
                        "current_price": 800000,
                        "price_timeline": [
                            {
                                "date": datetime.now(),
                                "price": 800000
                            }
                        ]
                    }
                ]
            }
        ]
        
        self.price_tracker.price_history_permanent_collection.aggregate.return_value.to_list.return_value = mock_aggregation_result
        
        with patch.object(self.price_tracker, '_calculate_property_metrics', return_value={"moving_averages": {}, "percent_changes": {}}):
            with patch.object(self.price_tracker, 'generate_permanent_id', return_value="test_id"):
                
                await self.price_tracker._create_city_price_snapshots()
        
        call = self.price_tracker.price_city_snapshot_collection.update_one.call_args_list[0]
        _, update = call[0]
        snapshot = update["$set"]
        
        # Verify current metrics
        current_metrics = snapshot["current_active_metrics"]
        assert current_metrics["condo"]["count"] == 0, "Should have 0 condos"
        assert current_metrics["condo"]["avg_price"] is None, "Condo avg price should be None"
        
        # Verify today's historical entry
        historical_averages = snapshot["historical_daily_averages"]
        today = datetime.now().date().isoformat()
        today_entry = next((e for e in historical_averages if e["date"] == today), None)
        
        assert today_entry is not None, "Today's entry should exist"
        assert today_entry["condo_listing_count"] == 0, "Today's condo count should be 0"
        assert today_entry["condo_avg_price"] is None, "Today's condo avg price should be None"
        assert today_entry["sfr_listing_count"] == 1, "Today's SFR count should be 1"
        assert today_entry["overall_listing_count"] == 1, "Today's overall count should be 1"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

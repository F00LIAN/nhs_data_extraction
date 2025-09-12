"""
Test City Snapshot Aggregation Functionality
Tests the city-level price aggregation logic, especially condominium handling
"""

import asyncio
import pytest
import logging
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.price_tracker import PriceTracker


class TestCitySnapshotAggregation:
    """Test city snapshot aggregation functionality"""
    
    def setup_method(self):
        """Setup test environment"""
        self.price_tracker = PriceTracker()
        
        # Mock MongoDB collections
        self.price_tracker.price_history_permanent_collection = AsyncMock()
        self.price_tracker.price_city_snapshot_collection = AsyncMock()
        
        # Sample data based on provided examples
        self.sample_permanent_records = [
            {
                "_id": "68c0af909e406be6990d5bd2",
                "permanent_property_id": "ac1efc6b95e170e74517563f592747a9",
                "accommodationCategory": "Single Family Residence",
                "address": {
                    "county": "Ventura County",
                    "addressLocality": "Ventura",
                    "addressRegion": "CA",
                    "streetAddress": "10767 Bridgeport Walk",
                    "postalCode": "93004"
                },
                "aggregated_metrics": {
                    "most_recent_price": 1360919,
                    "average_price": 1360919,
                    "min_price": 1360919,
                    "max_price": 1360919,
                    "total_days_tracked": 1
                },
                "community_id": "https://www.newhomesource.com/plan/plan-12-shea-homes-ventura-ca/3063205_Plan_12",
                "community_name": "Plan 12",
                "listing_status": "active",
                "offeredBy": "Shea Homes"
            },
            {
                "_id": "68c0af909e406be6990d5bd3",
                "permanent_property_id": "bc2efc6b95e170e74517563f592747b0",
                "accommodationCategory": "Condominium",
                "address": {
                    "county": "Ventura County",
                    "addressLocality": "Ventura", 
                    "addressRegion": "CA",
                    "streetAddress": "12345 Condo Way",
                    "postalCode": "93004"
                },
                "aggregated_metrics": {
                    "most_recent_price": 750000,
                    "average_price": 750000,
                    "min_price": 750000,
                    "max_price": 750000,
                    "total_days_tracked": 1
                },
                "community_id": "https://www.newhomesource.com/condo/oceanview-towers/4567890_Unit_A",
                "community_name": "Oceanview Towers Unit A",
                "listing_status": "active",
                "offeredBy": "Coastal Builders"
            },
            {
                "_id": "68c0af909e406be6990d5bd4",
                "permanent_property_id": "dc3efc6b95e170e74517563f592747c1",
                "accommodationCategory": "Condominium",
                "address": {
                    "county": "Riverside County",
                    "addressLocality": "Beaumont",
                    "addressRegion": "CA", 
                    "streetAddress": "67890 Desert View Blvd",
                    "postalCode": "92223"
                },
                "aggregated_metrics": {
                    "most_recent_price": 425000,
                    "average_price": 425000,
                    "min_price": 425000,
                    "max_price": 425000,
                    "total_days_tracked": 1
                },
                "community_id": "https://www.newhomesource.com/condo/desert-springs/7891234_Unit_B",
                "community_name": "Desert Springs Unit B",
                "listing_status": "active",
                "offeredBy": "Desert Homes"
            },
            {
                "_id": "68c0af909e406be6990d5bd5", 
                "permanent_property_id": "ed4efc6b95e170e74517563f592747d2",
                "accommodationCategory": "Single Family Residence",
                "address": {
                    "county": "Riverside County",
                    "addressLocality": "Beaumont",
                    "addressRegion": "CA",
                    "streetAddress": "11111 Valley Road",
                    "postalCode": "92223"
                },
                "aggregated_metrics": {
                    "most_recent_price": 550000,
                    "average_price": 550000,
                    "min_price": 550000,
                    "max_price": 550000,
                    "total_days_tracked": 1
                },
                "community_id": "https://www.newhomesource.com/plan/valley-homes/1234567_Model_C",
                "community_name": "Valley Homes Model C",
                "listing_status": "active",
                "offeredBy": "Valley Builders"
            }
        ]
    
    def mock_aggregation_pipeline_result(self):
        """Mock the MongoDB aggregation pipeline result"""
        return [
            {
                "_id": {
                    "city": "Ventura",
                    "county": "Ventura County", 
                    "postal_code": "93004"
                },
                "properties": [
                    {
                        "accommodation_category": "Single Family Residence",
                        "listing_status": "active",
                        "current_price": 1360919,
                        "price_timeline": []
                    },
                    {
                        "accommodation_category": "Condominium",
                        "listing_status": "active", 
                        "current_price": 750000,
                        "price_timeline": []
                    }
                ]
            },
            {
                "_id": {
                    "city": "Beaumont",
                    "county": "Riverside County",
                    "postal_code": "92223"
                },
                "properties": [
                    {
                        "accommodation_category": "Condominium",
                        "listing_status": "active",
                        "current_price": 425000,
                        "price_timeline": []
                    },
                    {
                        "accommodation_category": "Single Family Residence", 
                        "listing_status": "active",
                        "current_price": 550000,
                        "price_timeline": []
                    }
                ]
            }
        ]
    
    @pytest.mark.asyncio
    async def test_city_aggregation_with_condos(self):
        """Test that city aggregation correctly processes condominiums"""
        
        # Mock the aggregation pipeline
        mock_aggregation_result = self.mock_aggregation_pipeline_result()
        self.price_tracker.price_history_permanent_collection.aggregate.return_value.to_list.return_value = mock_aggregation_result
        
        # Mock the helper methods
        with patch.object(self.price_tracker, '_calculate_historical_daily_averages', return_value=[]):
            with patch.object(self.price_tracker, '_calculate_property_metrics', return_value={"moving_averages": {}, "percent_changes": {}}):
                with patch.object(self.price_tracker, 'generate_permanent_id', side_effect=lambda x: f"test_id_{hash(x) % 1000}"):
                    
                    # Execute the method
                    await self.price_tracker._create_city_price_snapshots()
        
        # Verify that update_one was called for each city
        assert self.price_tracker.price_city_snapshot_collection.update_one.call_count == 2
        
        # Extract the calls made to update_one
        calls = self.price_tracker.price_city_snapshot_collection.update_one.call_args_list
        
        # Test Ventura city data
        ventura_call = calls[0]
        ventura_filter, ventura_update = ventura_call[0]
        ventura_snapshot = ventura_update["$set"]
        
        assert ventura_snapshot["addressLocality"] == "Ventura"
        assert ventura_snapshot["county"] == "Ventura County"
        
        # Check SFR metrics
        sfr_metrics = ventura_snapshot["current_active_metrics"]["sfr"]
        assert sfr_metrics["count"] == 1
        assert sfr_metrics["avg_price"] == 1360919
        
        # Check Condo metrics - THIS IS THE KEY TEST
        condo_metrics = ventura_snapshot["current_active_metrics"]["condo"]
        assert condo_metrics["count"] == 1, f"Expected 1 condo, got {condo_metrics['count']}"
        assert condo_metrics["avg_price"] == 750000, f"Expected condo price 750000, got {condo_metrics['avg_price']}"
        
        # Check overall metrics
        overall_metrics = ventura_snapshot["current_active_metrics"]["overall"]
        assert overall_metrics["total_properties"] == 2
        assert overall_metrics["avg_price"] == (1360919 + 750000) / 2
        
        # Test Beaumont city data
        beaumont_call = calls[1]
        beaumont_filter, beaumont_update = beaumont_call[0]
        beaumont_snapshot = beaumont_update["$set"]
        
        assert beaumont_snapshot["addressLocality"] == "Beaumont"
        assert beaumont_snapshot["county"] == "Riverside County"
        
        # Check Beaumont SFR metrics
        beaumont_sfr = beaumont_snapshot["current_active_metrics"]["sfr"]
        assert beaumont_sfr["count"] == 1
        assert beaumont_sfr["avg_price"] == 550000
        
        # Check Beaumont Condo metrics
        beaumont_condo = beaumont_snapshot["current_active_metrics"]["condo"]
        assert beaumont_condo["count"] == 1, f"Expected 1 condo in Beaumont, got {beaumont_condo['count']}"
        assert beaumont_condo["avg_price"] == 425000, f"Expected Beaumont condo price 425000, got {beaumont_condo['avg_price']}"
    
    @pytest.mark.asyncio
    async def test_city_aggregation_no_condos(self):
        """Test city aggregation when no condos are present"""
        
        # Mock aggregation result with only SFR properties
        mock_result = [
            {
                "_id": {
                    "city": "TestCity",
                    "county": "Test County",
                    "postal_code": "12345"
                },
                "properties": [
                    {
                        "accommodation_category": "Single Family Residence",
                        "listing_status": "active",
                        "current_price": 500000,
                        "price_timeline": []
                    }
                ]
            }
        ]
        
        self.price_tracker.price_history_permanent_collection.aggregate.return_value.to_list.return_value = mock_result
        
        with patch.object(self.price_tracker, '_calculate_historical_daily_averages', return_value=[]):
            with patch.object(self.price_tracker, '_calculate_property_metrics', return_value={"moving_averages": {}, "percent_changes": {}}):
                with patch.object(self.price_tracker, 'generate_permanent_id', return_value="test_id"):
                    
                    await self.price_tracker._create_city_price_snapshots()
        
        # Verify update was called
        assert self.price_tracker.price_city_snapshot_collection.update_one.call_count == 1
        
        call = self.price_tracker.price_city_snapshot_collection.update_one.call_args_list[0]
        _, update = call[0]
        snapshot = update["$set"]
        
        # Check that condo metrics are properly null/zero
        condo_metrics = snapshot["current_active_metrics"]["condo"]
        assert condo_metrics["count"] == 0
        assert condo_metrics["avg_price"] is None
    
    @pytest.mark.asyncio
    async def test_city_aggregation_archived_condos(self):
        """Test that archived condos are not included in active metrics"""
        
        # Mock result with archived condo
        mock_result = [
            {
                "_id": {
                    "city": "TestCity",
                    "county": "Test County", 
                    "postal_code": "12345"
                },
                "properties": [
                    {
                        "accommodation_category": "Single Family Residence",
                        "listing_status": "active",
                        "current_price": 500000,
                        "price_timeline": []
                    },
                    {
                        "accommodation_category": "Condominium",
                        "listing_status": "archived",  # This should be filtered out
                        "current_price": 300000,
                        "price_timeline": []
                    }
                ]
            }
        ]
        
        self.price_tracker.price_history_permanent_collection.aggregate.return_value.to_list.return_value = mock_result
        
        with patch.object(self.price_tracker, '_calculate_historical_daily_averages', return_value=[]):
            with patch.object(self.price_tracker, '_calculate_property_metrics', return_value={"moving_averages": {}, "percent_changes": {}}):
                with patch.object(self.price_tracker, 'generate_permanent_id', return_value="test_id"):
                    
                    await self.price_tracker._create_city_price_snapshots()
        
        call = self.price_tracker.price_city_snapshot_collection.update_one.call_args_list[0]
        _, update = call[0]
        snapshot = update["$set"]
        
        # Verify archived condo is not counted in active metrics
        condo_metrics = snapshot["current_active_metrics"]["condo"]
        assert condo_metrics["count"] == 0, "Archived condos should not be counted in active metrics"
        assert condo_metrics["avg_price"] is None
        
        # But SFR should still be counted
        sfr_metrics = snapshot["current_active_metrics"]["sfr"]
        assert sfr_metrics["count"] == 1
        assert sfr_metrics["avg_price"] == 500000
    
    @pytest.mark.asyncio 
    async def test_property_type_variation_handling(self):
        """Test handling of property type string variations"""
        
        # Test with various string formats
        mock_result = [
            {
                "_id": {
                    "city": "TestCity", 
                    "county": "Test County",
                    "postal_code": "12345"
                },
                "properties": [
                    {
                        "accommodation_category": "Single Family Residence",  # Standard
                        "listing_status": "active",
                        "current_price": 500000,
                        "price_timeline": []
                    },
                    {
                        "accommodation_category": "Condominium",  # Standard 
                        "listing_status": "active",
                        "current_price": 300000,
                        "price_timeline": []
                    },
                    {
                        "accommodation_category": " Condominium ",  # With spaces - should not match
                        "listing_status": "active", 
                        "current_price": 250000,
                        "price_timeline": []
                    },
                    {
                        "accommodation_category": "condo",  # Different case - should not match
                        "listing_status": "active",
                        "current_price": 275000,
                        "price_timeline": []
                    }
                ]
            }
        ]
        
        self.price_tracker.price_history_permanent_collection.aggregate.return_value.to_list.return_value = mock_result
        
        with patch.object(self.price_tracker, '_calculate_historical_daily_averages', return_value=[]):
            with patch.object(self.price_tracker, '_calculate_property_metrics', return_value={"moving_averages": {}, "percent_changes": {}}):
                with patch.object(self.price_tracker, 'generate_permanent_id', return_value="test_id"):
                    
                    await self.price_tracker._create_city_price_snapshots()
        
        call = self.price_tracker.price_city_snapshot_collection.update_one.call_args_list[0]
        _, update = call[0]
        snapshot = update["$set"]
        
        # Only exact matches should be counted
        condo_metrics = snapshot["current_active_metrics"]["condo"]
        assert condo_metrics["count"] == 1, "Only exact 'Condominium' matches should be counted"
        assert condo_metrics["avg_price"] == 300000
        
        sfr_metrics = snapshot["current_active_metrics"]["sfr"]
        assert sfr_metrics["count"] == 1
        assert sfr_metrics["avg_price"] == 500000
        
        # Overall should include all active properties
        overall_metrics = snapshot["current_active_metrics"]["overall"]
        assert overall_metrics["total_properties"] == 4, "All active properties should be counted in overall"


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])

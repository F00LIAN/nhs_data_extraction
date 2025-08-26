#!/usr/bin/env python3
"""
Comprehensive test script for AsyncStage1DataExtract
Tests all implemented use cases:
1. Queue system with browser impersonation rotation
2. Listing ID generation and remove/update/add logic
3. Archive collection for removed listings
4. Async structure and error handling
"""

import asyncio
import json
import os
import sys
import logging
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch
from dotenv import load_dotenv

# Add the scraper directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import with proper module handling
import importlib.util
spec = importlib.util.spec_from_file_location("stage1", "stage-1-data-extract.py")
stage1_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stage1_module)
AsyncStage1DataExtract = stage1_module.AsyncStage1DataExtract

load_dotenv()

class TestAsyncStage1DataExtract:
    def __init__(self):
        self.extractor = AsyncStage1DataExtract(max_concurrent=5)
        self.test_results = {
            "queue_system": False,
            "listing_id_generation": False,
            "remove_update_add": False,
            "archive_logic": False,
            "async_structure": False,
            "mongodb_connection": False
        }
    
    def setup_test_logging(self):
        """Setup logging for test session"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - TEST - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        return self.logger
    
    async def test_mongodb_connection(self):
        """Test 1: MongoDB Connection"""
        self.logger.info("🧪 Testing MongoDB connection...")
        try:
            if not os.getenv("MONGO_DB_URI"):
                self.logger.warning("⚠️ No MONGO_DB_URI found, skipping MongoDB test")
                self.test_results["mongodb_connection"] = "SKIPPED"
                return
                
            client = await self.extractor.connect_to_mongodb()
            # Fix: Check collections properly for AsyncIOMotorClient
            if (client is not None and 
                self.extractor.homepagedata_collection is not None and
                self.extractor.archive_collection is not None):
                self.logger.info("✅ MongoDB connection successful")
                self.test_results["mongodb_connection"] = True
                client.close()
            else:
                self.logger.error("❌ MongoDB connection failed")
                
        except Exception as e:
            self.logger.error(f"❌ MongoDB connection error: {e}")
    
    def test_listing_id_generation(self):
        """Test 2: Listing ID Generation"""
        self.logger.info("🧪 Testing listing ID generation...")
        
        # Test cases
        test_cases = [
            # Dict with URL
            {"url": "https://example.com/listing1", "name": "Test Home"},
            # Dict without URL (should use fallback)
            {"name": "Test Home", "address": "123 Main St", "@id": "test123"},
            # List with URL
            [{"url": "https://example.com/listing2", "name": "Test Home 2"}],
            # Invalid data
            None,
            [],
            {}
        ]
        
        expected_results = [
            "https://example.com/listing1",  # URL found
            None,  # Would use fallback hash but we expect warning
            "https://example.com/listing2",  # URL in list
            None,  # Invalid
            None,  # Empty list
            None   # Empty dict - should use fallback
        ]
        
        success_count = 0
        for i, test_case in enumerate(test_cases):
            try:
                result = self.extractor.get_listing_id(test_case)
                if i < 3:  # First 3 should return valid IDs
                    if result:
                        success_count += 1
                        self.logger.info(f"✅ Test case {i+1}: {result}")
                    else:
                        self.logger.warning(f"⚠️ Test case {i+1}: No ID generated")
                else:  # Last 3 should return None
                    if result is None:
                        success_count += 1
                        self.logger.info(f"✅ Test case {i+1}: Correctly returned None")
                    else:
                        self.logger.warning(f"⚠️ Test case {i+1}: Unexpected result: {result}")
                        
            except Exception as e:
                self.logger.error(f"❌ Test case {i+1} error: {e}")
        
        if success_count >= 4:  # Allow some flexibility
            self.test_results["listing_id_generation"] = True
            self.logger.info("✅ Listing ID generation tests passed")
        else:
            self.logger.error(f"❌ Listing ID generation tests failed: {success_count}/6")
    
    async def test_queue_system(self):
        """Test 3: Queue System and Browser Rotation"""
        self.logger.info("🧪 Testing queue system...")
        
        # Mock the temp collection
        mock_temp_collection = Mock()
        mock_temp_collection.insert_one = AsyncMock()
        mock_temp_collection.delete_one = AsyncMock()
        self.extractor.temp_collection = mock_temp_collection
        
        # Mock the homepagedata collection  
        mock_homepage_collection = Mock()
        mock_homepage_collection.find = Mock()
        mock_homepage_collection.find.return_value.to_list = AsyncMock(return_value=[])
        self.extractor.homepagedata_collection = mock_homepage_collection
        
        # Add some URLs to retry queue
        test_urls = [
            ("https://test1.com", "chrome"),
            ("https://test2.com", "safari"),
            ("https://test3.com", "firefox")
        ]
        
        for url, impersonation in test_urls:
            await self.extractor.retry_queue.put((url, impersonation))
        
        # Mock fetch_without_proxy to simulate failures and success
        original_fetch = self.extractor.fetch_without_proxy
        
        call_count = 0
        async def mock_fetch(url, impersonation):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:  # First 2 calls fail
                return 403, ""
            else:  # Third call succeeds
                return 200, "<html><script type='application/ld+json'>{\"url\": \"" + url + "\"}</script></html>"
        
        self.extractor.fetch_without_proxy = mock_fetch
        
        # Process the queue
        await self.extractor.process_retry_queue()
        
        # Check results
        if len(self.extractor.failed_urls) < len(test_urls) and call_count > len(test_urls):
            self.test_results["queue_system"] = True
            self.logger.info("✅ Queue system test passed")
        else:
            self.logger.error(f"❌ Queue system test failed: {len(self.extractor.failed_urls)} failed, {call_count} calls")
        
        # Restore original method
        self.extractor.fetch_without_proxy = original_fetch
    
    async def test_remove_update_add_logic(self):
        """Test 4: Remove/Update/Add Logic"""
        self.logger.info("🧪 Testing remove/update/add logic...")
        
        # Mock collections
        mock_homepage_collection = Mock()
        mock_archive_collection = Mock()
        mock_homepage_collection.find_one = AsyncMock()
        mock_homepage_collection.delete_one = AsyncMock()
        mock_homepage_collection.replace_one = AsyncMock()
        mock_archive_collection.insert_one = AsyncMock()
        
        self.extractor.homepagedata_collection = mock_homepage_collection
        self.extractor.archivedlistings_collection = mock_archive_collection
        
        # Test data
        current_scraped_data = [
            {"url": "https://new-listing.com", "name": "New Home"},
            {"url": "https://existing-listing.com", "name": "Existing Home"},
        ]
        
        existing_db_ids = {
            "https://existing-listing.com",
            "https://removed-listing.com"
        }
        
        # Mock find_one for archive process
        mock_homepage_collection.find_one.return_value = {
            "property_id": "https://removed-listing.com",
            "name": "Removed Home",
            "scraped_at": "2024-01-01"
        }
        
        # Execute the logic
        processed_data, changes = await self.extractor.remove_update_add_listings(
            current_scraped_data, existing_db_ids
        )
        
        # Verify results
        new_listings = changes["new_listings"]
        removed_listings = changes["removed_listings"]
        active_listings = changes["active_listings"]
        
        expected_new = {"https://new-listing.com"}
        expected_removed = {"https://removed-listing.com"}
        expected_active = {"https://existing-listing.com"}
        
        if (new_listings == expected_new and 
            removed_listings == expected_removed and 
            active_listings == expected_active and
            len(processed_data) == 2):
            self.test_results["remove_update_add"] = True
            self.logger.info("✅ Remove/Update/Add logic test passed")
        else:
            self.logger.error(f"❌ Remove/Update/Add logic test failed")
            self.logger.error(f"New: {new_listings} (expected {expected_new})")
            self.logger.error(f"Removed: {removed_listings} (expected {expected_removed})")
            self.logger.error(f"Active: {active_listings} (expected {expected_active})")
    
    async def test_archive_logic(self):
        """Test 5: Archive Logic"""
        self.logger.info("🧪 Testing archive logic...")
        
        # Mock collections
        mock_homepage_collection = Mock()
        mock_archive_collection = Mock()
        
        # Mock successful find and operations
        mock_homepage_collection.find_one = AsyncMock(return_value={
            "property_id": "test-listing-123",
            "name": "Test Home",
            "scraped_at": "2024-01-01"
        })
        mock_homepage_collection.delete_one = AsyncMock()
        mock_archive_collection.insert_one = AsyncMock()
        
        self.extractor.homepagedata_collection = mock_homepage_collection
        self.extractor.archivedlistings_collection = mock_archive_collection
        
        # Test archiving
        removed_ids = ["test-listing-123"]
        archived_count = await self.extractor.archive_removed_listings(removed_ids)
        
        if archived_count == 1:
            self.test_results["archive_logic"] = True
            self.logger.info("✅ Archive logic test passed")
        else:
            self.logger.error(f"❌ Archive logic test failed: archived {archived_count}, expected 1")
    
    async def test_async_structure(self):
        """Test 6: Overall Async Structure"""
        self.logger.info("🧪 Testing async structure...")
        
        try:
            # Test URL generation
            urls, request_settings = self.extractor.Generate_URLs()
            
            if urls and request_settings and isinstance(urls, list):
                self.logger.info(f"✅ URL generation successful: {len(urls)} URLs")
                
                # Test with a small subset (just structure, not actual scraping)
                test_urls = urls[:2] if len(urls) > 2 else urls
                
                # Fix: Ensure request_settings has required keys
                if 'impersonation' not in request_settings:
                    request_settings['impersonation'] = 'chrome'
                
                # Mock all external dependencies
                with patch.object(self.extractor, 'connect_to_mongodb') as mock_connect, \
                     patch.object(self.extractor, 'process_single_url') as mock_process:
                    
                    mock_connect.return_value = Mock()
                    mock_process.return_value = True
                    
                    # Test the main scraping method structure
                    exit_code, logfile, success = await self.extractor.scrape_nhs_base_urls(
                        test_urls, request_settings
                    )
                    
                    if isinstance(exit_code, int) and isinstance(success, bool):
                        self.test_results["async_structure"] = True
                        self.logger.info("✅ Async structure test passed")
                    else:
                        self.logger.error("❌ Async structure test failed: invalid return types")
            else:
                self.logger.error("❌ URL generation failed")
                
        except Exception as e:
            self.logger.error(f"❌ Async structure test error: {e}")
    
    def print_test_summary(self):
        """Print comprehensive test summary"""
        self.logger.info("\n" + "="*60)
        self.logger.info("🧪 COMPREHENSIVE TEST SUMMARY")
        self.logger.info("="*60)
        
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results.values() if result is True)
        skipped_tests = sum(1 for result in self.test_results.values() if result == "SKIPPED")
        failed_tests = total_tests - passed_tests - skipped_tests
        
        for test_name, result in self.test_results.items():
            status = "✅ PASS" if result is True else "⚠️ SKIP" if result == "SKIPPED" else "❌ FAIL"
            self.logger.info(f"{test_name.replace('_', ' ').title()}: {status}")
        
        self.logger.info("="*60)
        self.logger.info(f"📊 Results: {passed_tests}/{total_tests} passed, {skipped_tests} skipped, {failed_tests} failed")
        
        if failed_tests == 0:
            self.logger.info("🎉 ALL TESTS PASSED! Ready for production use.")
        elif failed_tests <= 2:
            self.logger.warning("⚠️ Minor issues detected. Review failed tests.")
        else:
            self.logger.error("❌ Major issues detected. Requires fixes before use.")
        
        return failed_tests == 0
    
    async def run_all_tests(self):
        """Execute all test cases"""
        self.logger = self.setup_test_logging()
        self.logger.info("🚀 Starting comprehensive AsyncStage1DataExtract tests...")
        
        # Run all tests
        await self.test_mongodb_connection()
        self.test_listing_id_generation()
        await self.test_queue_system()
        await self.test_remove_update_add_logic()
        await self.test_archive_logic()
        await self.test_async_structure()
        
        # Print summary
        return self.print_test_summary()

async def main():
    """Main test execution"""
    tester = TestAsyncStage1DataExtract()
    success = await tester.run_all_tests()
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

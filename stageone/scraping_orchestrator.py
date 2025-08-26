"""
Scraping Orchestrator Module
Main coordinator that manages the entire scraping workflow.

Input: URLs with location info, request settings
Output: Scraping session results and statistics
Description: Orchestrates HTTP fetching, parsing, database operations, and logging for complete scraping workflow.
"""

import asyncio
import logging
import os
import time
from datetime import datetime
from typing import Dict, List, Tuple, Set

from .database_manager import DatabaseManager
from .listing_parser import ListingParser
from .http_fetcher import HttpFetcher

class ScrapingOrchestrator:
    def __init__(self, max_concurrent: int = 5, delay_between_requests: float = 5.0):
        self.database_manager = DatabaseManager()
        self.listing_parser = ListingParser()
        self.http_fetcher = HttpFetcher(max_concurrent, delay_between_requests)
        
        self.session_stats = {
            "success_count": 0,
            "error_count": 0,
            "new_listings": 0,
            "updated_listings": 0,
            "archived_listings": 0,
            "total_scraped_listings": set()
        }

    def setup_logging(self) -> Tuple[logging.Logger, str]:
        """
        Input: None
        Output: Tuple of (logger instance, log filename)
        Description: Configure session logging with timestamp-based filename
        """
        script_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(script_dir, "..", "logging", "nhs")
        os.makedirs(log_dir, exist_ok=True)
        
        log_filename = f"scraper_log_{time.strftime('%Y%m%d_%H%M%S')}.log"
        log_filepath = os.path.join(log_dir, log_filename)
        
        file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        logger = logging.getLogger(f"{__name__}_{id(self)}")
        logger.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)
        
        return logger, log_filename

    async def execute_scraping_session(self, urls_with_info: List, request_settings: Dict) -> Tuple[int, str, bool]:
        """
        Input: List of URLs with location info, request settings dict
        Output: Tuple of (exit_code, log_filename, success_boolean)
        Description: Main scraping workflow coordinator - handles full session from start to finish
        """
        logger, logfile = self.setup_logging()
        
        logger.info("🚀 Starting NewHomeSource scraping session")
        logger.info(f"🌐 Browser impersonation: {request_settings.get('impersonation', 'chrome')}")
        
        # Parse URL format - handle both plain URLs and (url, location_info) tuples
        if urls_with_info and isinstance(urls_with_info[0], tuple):
            urls = [url_info[0] for url_info in urls_with_info]
            url_location_map = {url_info[0]: url_info[1] for url_info in urls_with_info}
        else:
            urls = urls_with_info
            url_location_map = {}
        
        logger.info(f"📊 Total URLs to process: {len(urls)}")
        
        # Connect to database
        if not await self.database_manager.connect():
            logger.error("❌ Database connection failed")
            return 1, logfile, False
        
        # Get existing listings for comparison
        existing_listings = await self.database_manager.get_existing_listing_ids()
        logger.info(f"📊 Found {len(existing_listings)} existing listings in database")
        
        # Process all URLs
        all_scraped_listings = await self._process_urls(urls, url_location_map, logger)
        
        # Handle retry queues
        await self._process_retry_queues(logger)
        
        # Archive missing listings
        archived_count = await self._archive_missing_listings(existing_listings, all_scraped_listings, logger)
        
        # Generate final summary
        overall_success = self._generate_session_summary(urls, archived_count, logger)
        
        # Cleanup
        await self._cleanup_session(logger)
        
        exit_code = 0 if overall_success else 1
        return exit_code, logfile, overall_success

    async def _process_urls(self, urls: List[str], url_location_map: Dict, logger) -> Set[str]:
        """
        Input: URL list, location mapping, logger
        Output: Set of successfully scraped listing IDs
        Description: Process all URLs with rotating impersonation
        """
        all_scraped_listings = set()
        
        for idx, url in enumerate(urls, 1):
            try:
                # Rotate impersonation for first attempts
                current_impersonation = self.http_fetcher.get_next_impersonation()
                
                logger.info(f"🔍 Processing {idx}/{len(urls)}: {url} (using {current_impersonation})")
                
                # Fetch HTML content
                success, html = await self.http_fetcher.process_url_with_retry(
                    url, current_impersonation, url_location_map.get(url, {})
                )
                
                if success and html:
                    # Parse listings from HTML
                    location_info = url_location_map.get(url, {})
                    extracted_data = self.listing_parser.parse_html_content(url, html, location_info)
                    
                    if extracted_data:
                        # Store temporary HTML
                        temp_id = f"{url}_{int(datetime.now().timestamp())}_{idx}"
                        await self.database_manager.store_temp_html(temp_id, url, html)
                        
                        # Process batch and update database
                        scraped_ids = await self.database_manager.process_listing_batch(extracted_data)
                        all_scraped_listings.update(scraped_ids)
                        
                        # Clean up temp storage
                        await self.database_manager.temp_collection.delete_one({"_id": temp_id})
                        
                        self.session_stats["success_count"] += 1
                        logger.info(f"✅ Successfully processed {url}")
                    else:
                        self.session_stats["error_count"] += 1
                        logger.warning(f"⚠️ No listings found for {url}")
                else:
                    self.session_stats["error_count"] += 1
                    logger.error(f"❌ Failed to fetch {url}")
                
                # Rate limiting
                if idx < len(urls):
                    await asyncio.sleep(self.http_fetcher.delay_between_requests)
                    
            except Exception as e:
                logger.error(f"❌ Error processing {url}: {e}")
                self.session_stats["error_count"] += 1
        
        self.session_stats["total_scraped_listings"] = all_scraped_listings
        return all_scraped_listings

    async def _process_retry_queues(self, logger):
        """
        Input: Logger instance
        Output: None
        Description: Process retry queue and persistent failure retry
        """
        logger.info("🔄 Processing retry queue...")
        await self.http_fetcher.process_retry_queue()
        
        logger.info("🔄 Processing failure queue with persistent retry...")
        await self.http_fetcher.persistent_failure_retry(max_duration_minutes=30)
        
        # Save any remaining failed URLs
        self.http_fetcher.save_failed_urls()

    async def _archive_missing_listings(self, existing_listings: Set[str], all_scraped_listings: Set[str], logger) -> int:
        """
        Input: Set of existing listings, set of scraped listings, logger
        Output: Number of archived listings
        Description: Archive listings that are missing from current scrape with safety checks
        """
        missing_listings = existing_listings - all_scraped_listings
        archived_count = 0
        
        if missing_listings:
            missing_percentage = len(missing_listings) / len(existing_listings) if existing_listings else 0
            
            # Safety check: prevent mass archival
            if missing_percentage > 0.5:  # >50% missing
                logger.error(f"🚨 SAFETY CHECK TRIGGERED: {len(missing_listings)}/{len(existing_listings)} listings appear missing ({missing_percentage:.1%})")
                logger.error("🚨 This may indicate a data format change or scraping issue")
                logger.error("🚨 Skipping mass archival - manual investigation required")
                logger.error(f"🚨 Scraped listings found: {len(all_scraped_listings)}")
            else:
                logger.info(f"🗑️ Found {len(missing_listings)} missing listings - archiving immediately")
                archived_count = await self.database_manager.archive_missing_listings(missing_listings)
                logger.info(f"📦 Archived {archived_count} missing listings")
        else:
            logger.info("✅ No missing listings detected")
        
        self.session_stats["archived_listings"] = archived_count
        return archived_count

    def _generate_session_summary(self, urls: List[str], archived_count: int, logger) -> bool:
        """
        Input: URL list, archived count, logger
        Output: Overall success boolean
        Description: Generate comprehensive session summary and determine success
        """
        failure_stats = self.http_fetcher.get_failure_stats()
        
        logger.info(f"📊 SESSION COMPLETE:")
        logger.info(f"   ✅ Successful: {self.session_stats['success_count']}")
        logger.info(f"   ❌ Errors: {self.session_stats['error_count']}")
        logger.info(f"   ❌ Failed URLs: {failure_stats['failed_urls']}")
        logger.info(f"   📦 Archived: {archived_count}")
        logger.info(f"   🔍 Total scraped listings: {len(self.session_stats['total_scraped_listings'])}")
        
        # Success threshold: >50% successful and some listings found
        overall_success = (
            self.session_stats["success_count"] > 0 and 
            self.session_stats["error_count"] < len(urls) * 0.5 and
            len(self.session_stats["total_scraped_listings"]) > 0
        )
        
        status = "SUCCESS" if overall_success else "PARTIAL/FAILED"
        logger.info(f"🎯 Session status: {status}")
        
        return overall_success

    async def _cleanup_session(self, logger):
        """
        Input: Logger instance
        Output: None
        Description: Clean up resources and close connections
        """
        try:
            deleted_count = await self.database_manager.cleanup_temp_collection()
            logger.info(f"🧹 Cleanup complete")
        except Exception as e:
            logger.warning(f"⚠️ Cleanup warning: {e}")
        
        self.database_manager.close()
        logger.info("🔌 Database connection closed")

    def get_session_stats(self) -> Dict:
        """
        Input: None
        Output: Dictionary with session statistics
        Description: Return current session statistics for monitoring
        """
        return {
            **self.session_stats,
            "total_scraped_listings": len(self.session_stats["total_scraped_listings"]),
            "http_stats": self.http_fetcher.get_failure_stats()
        }

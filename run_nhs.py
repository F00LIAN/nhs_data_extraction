#!/usr/bin/env python3
"""
NewHomeSource Data Extraction Runner - Consolidated Version
Executes both Stage 1 (listings) and Stage 2 (community data) with modular orchestrators
"""

import asyncio
import sys
import os
import argparse
import logging
from dotenv import load_dotenv

# Add the scraper directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Direct imports using the new modular structure
from stageone.scraping_orchestrator import ScrapingOrchestrator
from stageone.url_generator import URLGenerator
from stagetwo.orchestrator import Stage2Orchestrator
from stagetwo.data_fetcher import DataFetcher

load_dotenv()

class NewHomeSourceExtractor:
    """Consolidated extractor that manages both Stage 1 and Stage 2"""
    
    def __init__(self, max_concurrent=10, browser_impersonation="chrome"):
        self.max_concurrent = max_concurrent
        self.browser_impersonation = browser_impersonation
        
        # Initialize Stage 1 orchestrator
        self.stage1_orchestrator = ScrapingOrchestrator(
            max_concurrent=max_concurrent,
            delay_between_requests=5.0
        )
        
        # Initialize Stage 2 orchestrator (reduced concurrency)
        stage2_concurrent = max(1, max_concurrent // 2)
        self.stage2_orchestrator = Stage2Orchestrator(
            max_concurrent=stage2_concurrent,
            delay_between_requests=0.5,
            max_retries=3
        )
        
        # Data fetcher for Stage 2
        self.data_fetcher = DataFetcher()
        
        # URL generator for Stage 1
        self.url_generator = URLGenerator()
    
    def generate_urls_and_settings(self):
        """
        Generate URLs and request settings for Stage 1.
        
        Input: None
        Output: Tuple[List[urls_with_info], Dict[request_settings]]
        Description: Creates URL list and browser settings for Stage 1 scraping
        """
        # Generate URLs using the modular URL generator
        urls_with_info = self.url_generator.generate_urls()
        request_settings = self.url_generator.get_request_settings()
        
        # Override browser impersonation if specified
        request_settings['impersonation'] = self.browser_impersonation
        request_settings['max_concurrent'] = self.max_concurrent
        
        return urls_with_info, request_settings
    
    async def run_stage1_extraction(self):
        """
        Execute Stage 1 data extraction.
        
        Input: None
        Output: Tuple[int, str, bool] - (exit_code, log_filename, success)
        Description: Orchestrates Stage 1 listing extraction workflow
        """
        print("🚀 Initializing Stage 1 Listing Extraction...")
        print(f"📊 Max concurrent requests: {self.max_concurrent}")
        print(f"🌐 Browser impersonation: {self.browser_impersonation}")
        print("="*60)
        
        try:
            # Generate URLs and request settings
            print("🔗 Generating URLs...")
            urls_with_info, request_settings = self.generate_urls_and_settings()
            
            print(f"✅ Generated {len(urls_with_info)} URLs to scrape")
            if urls_with_info:
                print(f"📍 Locations: {len(set(url[1]['display_name'] for url in urls_with_info))}")
            
            print("\n🏃‍♂️ Starting Stage 1 Data Extraction...")
            print("="*60)
            
            # Execute the main scraping
            exit_code, logfile, success = await self.stage1_orchestrator.execute_scraping_session(
                urls_with_info,
                request_settings
            )
            
            print("\n" + "="*60)
            print("🏁 Stage 1 Extraction Complete!")
            print("="*60)
            print(f"📋 Log file: {logfile}")
            print(f"🎯 Success: {'✅ YES' if success else '❌ NO'}")
            print(f"🔢 Exit code: {exit_code}")
            
            if success:
                print("\n🎉 Stage 1 completed successfully!")
                print("📊 Check your MongoDB collections:")
                print("   - homepagedata: New and updated listings")
                print("   - archivedlistings: Removed listings")
                print("   - temphtml: Temporary processing data")
            else:
                print("\n⚠️ Stage 1 completed with issues. Check the log file for details.")
            
            return exit_code
            
        except Exception as e:
            print(f"\n❌ Fatal error during Stage 1 extraction: {e}")
            print("💡 Check your .env file has MONGO_DB_URI configured")
            return 1
    
    async def run_stage2_extraction(self):
        """
        Execute Stage 2 community data extraction.
        
        Input: None
        Output: int - exit_code
        Description: Orchestrates Stage 2 community extraction workflow
        """
        print("\n🚀 Initializing Stage 2 Community Extraction...")
        print(f"📊 Max concurrent requests: {self.stage2_orchestrator.http_client.max_concurrent}")
        print("="*60)
        
        try:
            # Get property data from Stage 1 results
            print("🔗 Fetching property data from Stage 1...")
            property_data = self.data_fetcher.get_property_data()
            
            if not property_data:
                print("❌ No property data found from Stage 1")
                print("💡 Make sure Stage 1 has run successfully first")
                return 1
            
            print(f"✅ Found {len(property_data)} properties to process")
            
            print("\n🏃‍♂️ Starting Stage 2 Community Extraction...")
            print("="*60)
            
            # Execute Stage 2 extraction
            result = await self.stage2_orchestrator.execute_stage2_extraction()
            
            print("\n" + "="*60)
            print("🏁 Stage 2 Extraction Complete!")
            print("="*60)
            
            if result["success"]:
                print("✅ Community data and price snapshots captured")
                print("📊 Check your MongoDB collections:")
                print("   - communitydata: Detailed community information")
                print("   - pricehistory: Price snapshots from today's scrape")
                print("   - price_history_permanent: Long-term price trends")
                print(f"\n📈 Processing Statistics:")
                stats = result.get("stats", {})
                for key, value in stats.items():
                    print(f"   {key}: {value}")
                return 0
            else:
                print(f"❌ Stage 2 failed: {result.get('error', 'Unknown error')}")
                return 1
            
        except Exception as e:
            print(f"\n❌ Fatal error during Stage 2 extraction: {e}")
            print("💡 Check your .env file and ensure Stage 1 completed successfully")
            return 1
    
    async def run_full_extraction(self):
        """
        Execute both Stage 1 and Stage 2 in sequence.
        
        Input: None
        Output: int - exit_code
        Description: Runs complete end-to-end extraction workflow
        """
        print("🎯 Starting Full NewHomeSource Data Extraction")
        print("📅 This will run Stage 1 followed by Stage 2")
        print("="*80)
        
        # Run Stage 1
        stage1_exit_code = await self.run_stage1_extraction()
        
        if stage1_exit_code != 0:
            print("❌ Stage 1 failed, skipping Stage 2")
            return stage1_exit_code
        
        # Run Stage 2
        stage2_exit_code = await self.run_stage2_extraction()
        
        print("\n" + "="*80)
        print("🎊 FULL EXTRACTION SUMMARY")
        print("="*80)
        print(f"📊 Stage 1 Status: {'✅ SUCCESS' if stage1_exit_code == 0 else '❌ FAILED'}")
        print(f"📊 Stage 2 Status: {'✅ SUCCESS' if stage2_exit_code == 0 else '❌ FAILED'}")
        
        if stage1_exit_code == 0 and stage2_exit_code == 0:
            print("\n🎉 Complete data extraction successful!")
            print("💰 Price tracking is now active and historical data preserved")
        else:
            print(f"\n⚠️ Extraction completed with issues (Exit codes: S1={stage1_exit_code}, S2={stage2_exit_code})")
        
        return max(stage1_exit_code, stage2_exit_code)

async def run_extraction(stage="full", max_concurrent=10, browser_impersonation="chrome"):
    """
    Main execution function for different extraction modes.
    
    Input: stage (str), max_concurrent (int), browser_impersonation (str)
    Output: int - exit_code
    Description: Coordinates extraction based on specified stage and settings
    """
    extractor = NewHomeSourceExtractor(max_concurrent, browser_impersonation)
    
    if stage == '1':
        print("🎯 Running Stage 1 only (listing extraction)")
        return await extractor.run_stage1_extraction()
    elif stage == '2':
        print("🎯 Running Stage 2 only (community extraction)")
        return await extractor.run_stage2_extraction()
    else:  # stage == 'full'
        print("🎯 Running full extraction (Stage 1 + Stage 2 + Price Tracking)")
        return await extractor.run_full_extraction()

def main():
    """Main execution with command line arguments"""
    parser = argparse.ArgumentParser(description='NewHomeSource Data Extraction with Price Tracking')
    parser.add_argument('--stage', 
                       choices=['1', '2', 'full'],
                       default='full',
                       help='Which stage to run: 1 (listings only), 2 (community data only), full (both stages)')
    parser.add_argument('--max-concurrent', 
                       type=int, 
                       default=10,
                       help='Maximum concurrent requests (default: 10)')
    parser.add_argument('--browser', 
                       choices=['chrome', 'firefox', 'safari', 'chrome_android', 'safari_ios'],
                       default='chrome',
                       help='Browser to impersonate (default: chrome)')
    parser.add_argument('--verbose', '-v',
                       action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set up logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Check environment
    if not os.getenv("MONGO_DB_URI"):
        print("❌ Error: MONGO_DB_URI not found in environment variables")
        print("💡 Please ensure your .env file is configured properly")
        return 1
    
    print("🔍 Environment check passed")
    
    # Run extraction
    exit_code = asyncio.run(run_extraction(
        stage=args.stage,
        max_concurrent=args.max_concurrent,
        browser_impersonation=args.browser
    ))
    
    return exit_code

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
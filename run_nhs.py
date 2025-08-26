#!/usr/bin/env python3
"""
Stage 1 Data Extraction Runner
Executes the AsyncStage1DataExtract with proper configuration
"""

import asyncio
import sys
import os
import argparse
from dotenv import load_dotenv

# Add the scraper directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import with proper module handling
import importlib.util

# Import Stage 1
spec1 = importlib.util.spec_from_file_location("stage1", "stage-1-listing-extract.py")
stage1_module = importlib.util.module_from_spec(spec1)
spec1.loader.exec_module(stage1_module)
AsyncStage1DataExtract = stage1_module.AsyncStage1DataExtract

# Import Stage 2
spec2 = importlib.util.spec_from_file_location("stage2", "stage-2-community-extract.py")
stage2_module = importlib.util.module_from_spec(spec2)
spec2.loader.exec_module(stage2_module)
AsyncProxyHandler = stage2_module.AsyncProxyHandler
get_property_data_from_mongodb = stage2_module.get_property_data_from_mongodb

load_dotenv()

async def run_stage1_extraction(max_concurrent=10, browser_impersonation="chrome"):
    """
    Execute Stage 1 data extraction
    """
    print("🚀 Initializing AsyncStage1DataExtract...")
    print(f"📊 Max concurrent requests: {max_concurrent}")
    print(f"🌐 Browser impersonation: {browser_impersonation}")
    print("="*60)
    
    # Initialize extractor
    extractor = AsyncStage1DataExtract(max_concurrent=max_concurrent)
    
    try:
        # Generate URLs and request settings
        print("🔗 Generating URLs...")
        urls, request_settings = extractor.Generate_URLs()
        
        # Override browser impersonation if specified
        if browser_impersonation != "chrome":
            request_settings['impersonation'] = browser_impersonation
        
        print(f"✅ Generated {len(urls)} URLs to scrape")
        print(f"📍 Locations: {len(set(url[1]['display_name'] for url in urls))}")
        
        print("\n🏃‍♂️ Starting Stage 1 Data Extraction...")
        print("="*60)
        
        # Execute the main scraping with full URL and location info
        exit_code, logfile, success = await extractor.scrape_nhs_base_urls(
            urls,  # Pass full tuples with location info
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

async def run_stage2_extraction(max_concurrent=10):
    """
    Execute Stage 2 community data extraction
    """
    print("\n🚀 Initializing Stage 2 Community Extraction...")
    print(f"📊 Max concurrent requests: {max_concurrent}")
    print("="*60)
    
    try:
        # Get property data from Stage 1 results
        print("🔗 Fetching property data from Stage 1...")
        property_data = get_property_data_from_mongodb()
        
        if not property_data:
            print("❌ No property data found from Stage 1")
            print("💡 Make sure Stage 1 has run successfully first")
            return 1
        
        print(f"✅ Found {len(property_data)} properties to process")
        
        # Initialize Stage 2 handler with reduced concurrency
        stage2_concurrent = max(1, max_concurrent // 2)  # Half of Stage 1 concurrency
        print(f"📊 Stage 2 concurrency: {stage2_concurrent} (half of Stage 1: {max_concurrent})")
        
        handler = AsyncProxyHandler(
            max_concurrent=stage2_concurrent,
            delay_between_requests=0.5,
            max_retries=3
        )
        
        print("\n🏃‍♂️ Starting Stage 2 Community Extraction...")
        print("="*60)
        
        # Execute Stage 2 extraction
        await handler.reach_target_property_urls_async(property_data)
        
        print("\n" + "="*60)
        print("🏁 Stage 2 Extraction Complete!")
        print("="*60)
        print("✅ Community data and price snapshots captured")
        print("📊 Check your MongoDB collections:")
        print("   - communitydata: Detailed community information")
        print("   - pricehistory: Price snapshots from today's scrape")
        print("   - price_history_permanent: Long-term price trends")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Fatal error during Stage 2 extraction: {e}")
        print("💡 Check your .env file and ensure Stage 1 completed successfully")
        return 1

async def run_full_extraction(max_concurrent=10, browser_impersonation="chrome"):
    """
    Execute both Stage 1 and Stage 2 in sequence
    """
    print("🎯 Starting Full NewHomeSource Data Extraction")
    print("📅 This will run Stage 1 followed by Stage 2")
    print("="*80)
    
    # Run Stage 1
    stage1_exit_code = await run_stage1_extraction(max_concurrent, browser_impersonation)
    
    if stage1_exit_code != 0:
        print("❌ Stage 1 failed, skipping Stage 2")
        return stage1_exit_code
    
    # Run Stage 2
    stage2_exit_code = await run_stage2_extraction(max_concurrent)
    
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
    
    args = parser.parse_args()
    
    # Check environment
    if not os.getenv("MONGO_DB_URI"):
        print("❌ Error: MONGO_DB_URI not found in environment variables")
        print("💡 Please ensure your .env file is configured properly")
        return 1
    
    print("🔍 Environment check passed")
    
    # Run based on stage selection
    if args.stage == '1':
        print("🎯 Running Stage 1 only (listing extraction)")
        exit_code = asyncio.run(run_stage1_extraction(
            max_concurrent=args.max_concurrent,
            browser_impersonation=args.browser
        ))
    elif args.stage == '2':
        print("🎯 Running Stage 2 only (community extraction)")
        exit_code = asyncio.run(run_stage2_extraction(
            max_concurrent=args.max_concurrent
        ))
    else:  # args.stage == 'full'
        print("🎯 Running full extraction (Stage 1 + Stage 2 + Price Tracking)")
        exit_code = asyncio.run(run_full_extraction(
            max_concurrent=args.max_concurrent,
            browser_impersonation=args.browser
        ))
    
    return exit_code

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

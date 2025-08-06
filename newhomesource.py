import curl_cffi
from bs4 import BeautifulSoup
import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os
from datetime import datetime
import hashlib
import logging
import sys
import io
import time
import random
import argparse
from url_generator import URLGenerator
from price_history import save_scraped_prices

# Force UTF-8 encoding for console output on Windows
if sys.platform.startswith('win'):
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# Load environment variables
load_dotenv()

class SafeConsoleHandler(logging.StreamHandler):
    """Custom StreamHandler that handles Unicode encoding errors gracefully"""
    
    # Emoji to ASCII fallback mapping
    EMOJI_FALLBACKS = {
        '🔄': '[SYNC]',
        '💾': '[SAVE]', 
        '🔍': '[SEARCH]',
        '🎉': '[SUCCESS]',
        '📋': '[LOG]',
        '🔌': '[CONN]',
        '✅': '[OK]',
        '❌': '[FAIL]',
        '⚠️': '[WARN]',
        '📊': '[STATS]',
        '🏠': '[HOMES]',
        'ℹ️': '[INFO]',
        '💥': '[ERROR]'
    }
    
    def emit(self, record):
        try:
            msg = self.format(record)
            # Try to encode the message to detect Unicode issues
            try:
                msg.encode('cp1252')
            except UnicodeEncodeError:
                # Replace emoji with ASCII fallbacks
                for emoji, fallback in self.EMOJI_FALLBACKS.items():
                    msg = msg.replace(emoji, fallback)
            
            stream = self.stream
            stream.write(msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)

# Configure logging
def setup_logging():
    """Setup logging configuration with proper Unicode handling
    
    Creates log files in the logging/nhs/ directory relative to the script location.
    """
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Create logging directory structure if it doesn't exist
    log_dir = os.path.join(script_dir, "logging", "nhs")
    os.makedirs(log_dir, exist_ok=True)
    
    log_filename = f"scraper_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_filepath = os.path.join(log_dir, log_filename)
    
    # Create file handler with UTF-8 encoding
    file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # Create console handler with error handling for encoding issues
    console_handler = SafeConsoleHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # Set formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Configure logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Clear any existing handlers to avoid duplicates
    logger.handlers = [file_handler, console_handler]
    
    # Return relative path from script directory for better user information
    relative_log_path = os.path.join("logging", "nhs", log_filename)
    return logger, relative_log_path

def get_property_id(property_data):
    """Generate unique identifier for property based on URL and key attributes"""
    if isinstance(property_data, dict):
        # Primary identifier: URL
        if "url" in property_data:
            return property_data["url"]
        # Fallback: hash of key property attributes
        key_fields = ["name", "address", "@id", "identifier"]
        id_parts = []
        for field in key_fields:
            if field in property_data:
                id_parts.append(str(property_data[field]))
        if id_parts:
            return hashlib.md5("|".join(id_parts).encode()).hexdigest()
    return None

def retry_request(url, session, max_retries=3, base_delay=1, max_delay=60, logger=None):
    """
    Retry HTTP requests with exponential backoff for handling rate limiting and temporary failures
    
    Args:
        url: URL to request
        session: curl_cffi Session object for maintaining state
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds for exponential backoff
        max_delay: Maximum delay in seconds
        logger: Logger instance for logging retry attempts
    
    Returns:
        response object or None if all retries failed
    """
    last_exception = None
    
    for attempt in range(max_retries + 1):  # +1 for initial attempt
        try:
            if logger and attempt > 0:
                logger.info(f"🔄 Retry attempt {attempt} for {url}")
            
            response = session.get(url, timeout=30)
            
            # Success for any 2xx status code
            if 200 <= response.status_code < 300:
                if logger and attempt > 0:
                    logger.info(f"✅ Request succeeded on attempt {attempt + 1}")
                return response
            
            # Handle specific error codes
            if response.status_code == 403:
                if logger:
                    logger.warning(f"⚠️ 403 Forbidden received (attempt {attempt + 1})")
            elif response.status_code == 429:
                if logger:
                    logger.warning(f"⚠️ 429 Rate Limited received (attempt {attempt + 1})")
            elif response.status_code >= 500:
                if logger:
                    logger.warning(f"⚠️ Server error {response.status_code} received (attempt {attempt + 1})")
            else:
                # For other 4xx errors, don't retry
                if logger:
                    logger.error(f"❌ HTTP {response.status_code} error - not retrying")
                return response
            
            # Don't sleep on the last attempt
            if attempt < max_retries:
                # Exponential backoff with jitter
                delay = min(base_delay * (2 ** attempt), max_delay)
                jitter = random.uniform(0.1, 0.3) * delay  # Add 10-30% jitter
                total_delay = delay + jitter
                
                if logger:
                    logger.info(f"💤 Waiting {total_delay:.1f}s before retry...")
                time.sleep(total_delay)
            
            return response
            
        except Exception as e:
            last_exception = e
            if logger:
                logger.warning(f"⚠️ Request exception on attempt {attempt + 1}: {e}")
            
            # Don't sleep on the last attempt
            if attempt < max_retries:
                delay = min(base_delay * (2 ** attempt), max_delay)
                jitter = random.uniform(0.1, 0.3) * delay
                total_delay = delay + jitter
                
                if logger:
                    logger.info(f"💤 Waiting {total_delay:.1f}s before retry...")
                time.sleep(total_delay)
    
    # All retries failed
    if logger:
        logger.error(f"❌ All {max_retries + 1} attempts failed for {url}")
        if last_exception:
            logger.error(f"❌ Last exception: {last_exception}")
    
    return None

def scrape_newhomesource(browser_impersonation="chrome"):
    """Main scraping function with comprehensive logging and error handling
    
    Args:
        browser_impersonation: Browser to impersonate ('chrome', 'firefox', 'safari', 'chrome_android', 'safari_ios')
    """
    logger, log_filename = setup_logging()
    
    # Track overall success/failure
    scraping_success = False
    db_success = False
    errors = []
    
    logger.info("=" * 50)
    logger.info("Starting NewHomeSource scraping session")
    logger.info(f"🌐 Browser impersonation: {browser_impersonation}")
    logger.info("=" * 50)
    
    # Create session for maintaining cookies and state, rotate impersonation if scraper fails
    try:
        session = curl_cffi.Session(impersonate=browser_impersonation)
        logger.info(f"🔌 Created curl_cffi session with {browser_impersonation} impersonation")
    except Exception as e:
        error_msg = f"❌ Failed to create session with {browser_impersonation}: {e}"
        logger.error(error_msg)
        errors.append(error_msg)
        return 1, log_filename, False
    
    # MongoDB connection setup
    uri = f"mongodb+srv://{os.getenv('MONGO_DB_USERNAME')}:{os.getenv('MONGO_DB_PASSWORD')}@newhomesourcedata.6gdo85y.mongodb.net/?retryWrites=true&w=majority&appName=NewHomeSourceData"
    
    try:
        client = MongoClient(uri, server_api=ServerApi('1'))
        client.admin.command('ping')
        logger.info("✅ Successfully connected to MongoDB!")
        
        db = client['newhomesource']
        collection = db['homepagedata']
        
        # Create indexes for efficient duplicate checking
        collection.create_index("property_id", unique=False)
        collection.create_index("scraped_at")
        logger.info("✅ Database indexes created/verified")
        
        # Get existing property IDs to avoid re-scraping
        existing_ids = set(doc["property_id"] for doc in collection.find({}, {"property_id": 1}) if "property_id" in doc)
        logger.info(f"📊 Found {len(existing_ids)} existing properties in database")
        
    except Exception as e:
        error_msg = f"❌ Failed to connect to MongoDB: {e}"
        logger.error(error_msg)
        errors.append(error_msg)
        client = None
        existing_ids = set()

    scraped_properties = set()
    all_scraped_documents = []  # Store ALL scraped data for price history
    new_documents = []
    pages_scraped = 0
    pages_with_errors = 0
    
    # Generate URLs using configuration
    url_generator = URLGenerator()
    urls_to_scrape = url_generator.generate_urls()
    request_settings = url_generator.get_request_settings()
    
    logger.info(f"📊 Generated {len(urls_to_scrape)} URLs across {len(set(url[1]['display_name'] for url in urls_to_scrape))} locations")
    
    # Scraping loop with retry logic and request delays
    for idx, (url, location_info) in enumerate(urls_to_scrape, 1):
        logger.info(f"🔍 Scraping {location_info['display_name']} ({idx}/{len(urls_to_scrape)}): {url}")
        
        # Add delay between requests (except for first request)
        if idx > 1:
            page_delay = random.uniform(request_settings['page_delay_min'], request_settings['page_delay_max'])
            logger.info(f"💤 Waiting {page_delay:.1f}s before next request...")
            time.sleep(page_delay)
        
        try:
            response = retry_request(url, session, 
                                   max_retries=request_settings['max_retries'], 
                                   base_delay=request_settings['base_delay'], 
                                   max_delay=request_settings['max_delay'], 
                                   logger=logger)
            
            if response is None:
                error_msg = f"❌ Failed to get response for {url} after all retries"
                logger.error(error_msg)
                pages_with_errors += 1
                continue
            
            if response.status_code != 200:
                error_msg = f"❌ HTTP {response.status_code} error for {url}"
                logger.warning(error_msg)
                pages_with_errors += 1
                continue
            
            soup = BeautifulSoup(response.text, 'html.parser')
            scripts = soup.find_all('script', type='application/ld+json')
            
            page_new_count = 0
            for script in scripts:
                try:
                    data = json.loads(script.text)
                    property_id = get_property_id(data)
                    
                    if property_id and property_id not in scraped_properties:
                        scraped_properties.add(property_id)
                        
                        # Extract county from display_name (e.g., "Riverside County, CA" -> "Riverside County")
                        county = location_info["display_name"].split(",")[0].strip()
                        
                        document = {
                            "property_id": property_id,
                            "scraped_at": datetime.now(),
                            "source_url": url,
                            "location_info": location_info,
                            "county": county,
                            "property_data": data
                        }
                        
                        # Add to ALL scraped documents for price history
                        all_scraped_documents.append(document)
                        
                        # Only add to new_documents if not a duplicate
                        if property_id not in existing_ids:
                            new_documents.append(document)
                            page_new_count += 1
                        
                except json.JSONDecodeError as e:
                    logger.debug(f"⚠️ JSON decode error for {url}: {e}")
                    continue
            
            logger.info(f"✅ {location_info['display_name']}: {page_new_count} new properties found")
            pages_scraped += 1
            
        except Exception as e:
            error_msg = f"❌ Error scraping {url}: {e}"
            logger.error(error_msg)
            errors.append(error_msg)
            pages_with_errors += 1
            continue

    # Determine scraping success
    scraping_success = pages_scraped > 0 and pages_with_errors < pages_scraped
    
    # Save price history FIRST (all scraped properties, including duplicates)
    if client and all_scraped_documents:
        try:
            logger.info("📊 Saving price history for all scraped properties...")
            price_history_count = save_scraped_prices(all_scraped_documents, logger)
            logger.info(f"✅ Price history saved: {price_history_count} records")
        except Exception as e:
            logger.error(f"⚠️ Error saving price history: {e}")
    
    # Insert new documents to MongoDB
    if client and new_documents:
        try:
            result = collection.insert_many(new_documents, ordered=False)
            logger.info(f"✅ Inserted {len(result.inserted_ids)} new properties to MongoDB")
            db_success = True
        except Exception as e:
            error_msg = f"❌ Error inserting to MongoDB: {e}"
            logger.error(error_msg)
            errors.append(error_msg)
            db_success = False
    elif not client:
        logger.warning("⚠️ Skipping MongoDB insertion (no connection)")
    elif not new_documents:
        logger.info("ℹ️ No new documents to insert")
        db_success = True  # No new data is not a failure
    
    # Final summary and status
    logger.info("\n" + "=" * 50)
    logger.info("SCRAPING SESSION SUMMARY")
    logger.info("=" * 50)
    logger.info(f"📊 Pages scraped successfully: {pages_scraped}")
    logger.info(f"⚠️ Pages with errors: {pages_with_errors}")
    logger.info(f"🏠 New properties found for Home Page DB: {len(new_documents)}")
    logger.info(f"📊 Total properties scraped to Price History DB: {len(all_scraped_documents)}")
    logger.info(f"🔄 Duplicates avoided for Home Page DB: {len(existing_ids)} existing")
    logger.info(f"💾 Database operation: {'✅ SUCCESS' if db_success else '❌ FAILED'}")
    logger.info(f"🔍 Scraping operation: {'✅ SUCCESS' if scraping_success else '❌ FAILED'}")
    
    if errors:
        logger.error(f"❌ Total errors encountered: {len(errors)}")
        for error in errors:
            logger.error(f"  - {error}")
    
    # Overall success determination
    overall_success = scraping_success and (db_success or not client) and len(errors) == 0
    
    if overall_success:
        logger.info("🎉 OVERALL STATUS: SUCCESS")
        exit_code = 0
    else:
        logger.error("💥 OVERALL STATUS: FAILED")
        exit_code = 1
    
    logger.info(f"📋 Log file saved as: {log_filename}")
    logger.info("=" * 50)
    
    # Price history already saved above - no additional snapshots needed
    
    if client:
        client.close()
        logger.info("🔌 MongoDB connection closed")
    
    # Close session
    session.close()
    logger.info("🔌 curl_cffi session closed")
    
    return exit_code, log_filename, overall_success

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='NewHomeSource scraper with browser impersonation')
    parser.add_argument('--browser', 
                       choices=['chrome', 'firefox', 'safari', 'chrome_android', 'safari_ios'],
                       default='chrome',
                       help='Browser to impersonate (default: chrome)')
    
    args = parser.parse_args()
    
    exit_code, log_filename, success = scrape_newhomesource(browser_impersonation=args.browser)
    sys.exit(exit_code)


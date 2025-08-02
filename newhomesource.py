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
    """Setup logging configuration with proper Unicode handling"""
    log_filename = f"scraper_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Create file handler with UTF-8 encoding
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
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
    
    return logger, log_filename

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

def scrape_newhomesource():
    """Main scraping function with comprehensive logging and error handling"""
    logger, log_filename = setup_logging()
    
    # Track overall success/failure
    scraping_success = False
    db_success = False
    errors = []
    
    logger.info("=" * 50)
    logger.info("Starting NewHomeSource scraping session")
    logger.info("=" * 50)
    
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
    new_documents = []
    pages_scraped = 0
    pages_with_errors = 0
    
    # Scraping loop
    for page in range(1, 10):
        logger.info(f"🔍 Scraping page {page}...")
        
        try:
            response = curl_cffi.get(
                f"https://www.newhomesource.com/communities/ca/riverside-san-bernardino-area/menifee/page-{page}", 
                impersonate="chrome"
            )
            
            if response.status_code != 200:
                error_msg = f"❌ HTTP {response.status_code} error on page {page}"
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
                    
                    if property_id and property_id not in scraped_properties and property_id not in existing_ids:
                        scraped_properties.add(property_id)
                        
                        document = {
                            "property_id": property_id,
                            "scraped_at": datetime.now(),
                            "source_page": page,
                            "property_data": data
                        }
                        new_documents.append(document)
                        page_new_count += 1
                        
                except json.JSONDecodeError as e:
                    logger.debug(f"⚠️ JSON decode error on page {page}: {e}")
                    continue
            
            logger.info(f"✅ Page {page}: {page_new_count} new properties found")
            pages_scraped += 1
            
        except Exception as e:
            error_msg = f"❌ Error scraping page {page}: {e}"
            logger.error(error_msg)
            errors.append(error_msg)
            pages_with_errors += 1
            continue

    # Determine scraping success
    scraping_success = pages_scraped > 0 and pages_with_errors < pages_scraped
    
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
    
    # Save JSON backup
    try:
        with open("newhomesource_data.json", 'w', encoding='utf-8') as f:
            json.dump(new_documents, f, indent=2, ensure_ascii=False, default=str)
        logger.info("✅ JSON backup saved successfully")
    except Exception as e:
        error_msg = f"❌ Error saving JSON backup: {e}"
        logger.error(error_msg)
        errors.append(error_msg)
    
    # Final summary and status
    logger.info("\n" + "=" * 50)
    logger.info("SCRAPING SESSION SUMMARY")
    logger.info("=" * 50)
    logger.info(f"📊 Pages scraped successfully: {pages_scraped}")
    logger.info(f"⚠️ Pages with errors: {pages_with_errors}")
    logger.info(f"🏠 New properties found: {len(new_documents)}")
    logger.info(f"🔄 Duplicates avoided: {len(existing_ids)} existing")
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
    
    if client:
        client.close()
        logger.info("🔌 MongoDB connection closed")
    
    return exit_code, log_filename, overall_success

if __name__ == "__main__":
    exit_code, log_filename, success = scrape_newhomesource()
    sys.exit(exit_code)


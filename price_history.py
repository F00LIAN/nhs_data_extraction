import os
from datetime import datetime
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import hashlib
import logging
from dotenv import load_dotenv

load_dotenv()

def get_unique_property_id(property_data):
    """Generate unique property identifier using name + lat/lng"""
    try:
        name = property_data.get("name", "")
        geo = property_data.get("geo", {})
        lat = geo.get("latitude", "")
        lng = geo.get("longitude", "")
        
        if name and lat and lng:
            # Create consistent identifier from name + coordinates
            id_string = f"{name}|{lat}|{lng}"
            return hashlib.md5(id_string.encode()).hexdigest()
    except Exception:
        pass
    return None

def save_scraped_prices(scraped_documents, logger=None):
    """Save price history for ALL scraped properties (including duplicates)"""
    if logger:
        logger.info("📊 Saving price history for scraped properties...")
    
    try:
        # MongoDB connection
        uri = f"mongodb+srv://{os.getenv('MONGO_DB_USERNAME')}:{os.getenv('MONGO_DB_PASSWORD')}@newhomesourcedata.6gdo85y.mongodb.net/?retryWrites=true&w=majority&appName=NewHomeSourceData"
        client = MongoClient(uri, server_api=ServerApi('1'))
        
        db = client[os.getenv('MONGO_DB_NAME', 'newhomesource')]
        history_collection = db[os.getenv('MONGO_DB_PRICE_HISTORY', 'pricehistory')]
        
        # Create index for efficient queries
        history_collection.create_index([("property_unique_id", 1), ("recorded_at", -1)])
        
        snapshot_time = datetime.now()
        price_snapshots = []
        
        for doc in scraped_documents:
            try:
                property_data = doc.get("property_data", {})
                unique_id = get_unique_property_id(property_data)
                
                if not unique_id:
                    continue
                
                price_str = property_data.get("offers", {}).get("price", "")
                try:
                    price = float(price_str)
                    if price <= 0:
                        continue
                except (ValueError, TypeError):
                    continue
                
                # Extract location data
                address = property_data.get("address", {})
                geo = property_data.get("geo", {})
                
                snapshot = {
                    "property_unique_id": unique_id,
                    "property_name": property_data.get("name", ""),
                    "county": doc.get("county", ""),
                    "city": address.get("addressLocality", ""),
                    "price": price,
                    "latitude": geo.get("latitude", ""),
                    "longitude": geo.get("longitude", ""),
                    "recorded_at": snapshot_time,
                    "source_scraped_at": doc.get("scraped_at"),
                    "data_source": "newhomesource_scraper"
                }
                
                price_snapshots.append(snapshot)
                
            except Exception as e:
                if logger:
                    logger.debug(f"Error processing property for price history: {e}")
                continue
        
        # Insert price history
        if price_snapshots:
            result = history_collection.insert_many(price_snapshots)
            if logger:
                logger.info(f"✅ Saved {len(result.inserted_ids)} price history records")
        else:
            if logger:
                logger.info("ℹ️ No valid prices found for history")
        
        client.close()
        return len(price_snapshots)
        
    except Exception as e:
        if logger:
            logger.error(f"❌ Error saving price history: {e}")
        return 0

if __name__ == "__main__":
    # Setup basic logging for standalone execution
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    print("Price history module - run from scraper")
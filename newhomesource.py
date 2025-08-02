import curl_cffi
from bs4 import BeautifulSoup
import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os
from datetime import datetime
import hashlib

# Load environment variables
load_dotenv()

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
    # MongoDB connection setup
    uri = f"mongodb+srv://{os.getenv('MONGO_DB_USERNAME')}:{os.getenv('MONGO_DB_PASSWORD')}@newhomesourcedata.6gdo85y.mongodb.net/?retryWrites=true&w=majority&appName=NewHomeSourceData"
    
    try:
        client = MongoClient(uri, server_api=ServerApi('1'))
        client.admin.command('ping')
        print("Successfully connected to MongoDB!")
        
        db = client['newhomesource']
        collection = db['homepagedata']
        
        # Create indexes for efficient duplicate checking
        collection.create_index("property_id", unique=False)
        collection.create_index("scraped_at")
        
        # Get existing property IDs to avoid re-scraping
        existing_ids = set(doc["property_id"] for doc in collection.find({}, {"property_id": 1}) if "property_id" in doc)
        print(f"Found {len(existing_ids)} existing properties in database")
        
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        client = None
        existing_ids = set()

    scraped_properties = set()
    new_documents = []
    
    for page in range(1, 10):
        print(f"Scraping page {page}...")
        
        try:
            response = curl_cffi.get(
                f"https://www.newhomesource.com/communities/ca/riverside-san-bernardino-area/menifee/page-{page}", 
                impersonate="chrome"
            )
            
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
                        
                except json.JSONDecodeError:
                    continue
            
            print(f"Page {page}: {page_new_count} new properties found")
            
        except Exception as e:
            print(f"Error scraping page {page}: {e}")
            continue

    # Insert new documents to MongoDB
    if client and new_documents:
        try:
            # Use bulk insert with ordered=False to continue on duplicates
            result = collection.insert_many(new_documents, ordered=False)
            print(f"Inserted {len(result.inserted_ids)} new properties to MongoDB")
        except Exception as e:
            print(f"Error inserting to MongoDB: {e}")
    
    # Save JSON backup
    with open("newhomesource_data.json", 'w', encoding='utf-8') as f:
        json.dump(new_documents, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\nScraping Summary:")
    print(f"- New properties found: {len(new_documents)}")
    print(f"- Duplicates avoided: {len(scraped_properties)} scraped vs {len(existing_ids)} existing")
    print(f"- Data saved to: newhomesource_data.json")
    
    if client:
        client.close()

if __name__ == "__main__":
    scrape_newhomesource()


"""
Database Maintenance Script for Community Data Collection

Removes instances where community_data.communities.price contains letters (A-Z).
Only accepts digit-only strings in price fields.

Example of invalid data to remove: "From 1,000,000"
Example of valid data to keep: "1,000,000"
"""

import os
import re
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def connect_to_database():
    """Connect to MongoDB and return the communitydata collection."""
    try:
        uri = os.getenv("MONGO_DB_URI")
        database_name = os.getenv("MONGO_DB_NAME", "newhomesource")
        
        client = MongoClient(uri)
        db = client[database_name]
        collection = db['communitydata']
        
        # Test connection
        client.admin.command('ping')
        print("✅ Connected to MongoDB successfully")
        return collection
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def has_letters_in_price(price_value):
    """Check if price field contains any letters A-Z (case insensitive).
    Exception: 'Coming Soon' is allowed and will not be flagged for removal.
    """
    if not isinstance(price_value, str):
        return False
    
    # Exception: Allow "Coming Soon" (case insensitive)
    if price_value.lower().strip() == "coming soon":
        return False
    
    return bool(re.search(r'[A-Za-z]', price_value))

def clean_communitydata():
    """Remove documents with letters in price fields and return count."""
    collection = connect_to_database()
    if collection is None:
        return 0
    
    total_removed = 0
    
    # Find all documents in the collection
    documents = collection.find({})
    documents_to_remove = []
    
    for doc in documents:
        should_remove = False
        
        # Check if document has community_data.communities array
        if 'community_data' in doc and 'communities' in doc['community_data']:
            for community in doc['community_data']['communities']:
                if 'price' in community:
                    if has_letters_in_price(community['price']):
                        should_remove = True
                        print(f"Found invalid price: '{community['price']}' in document {doc['listing_id']}")
                        break
        
        if should_remove:
            documents_to_remove.append(doc['_id'])
    
    # Remove the documents
    if documents_to_remove:
        result = collection.delete_many({'_id': {'$in': documents_to_remove}})
        total_removed = result.deleted_count
        print(f"✅ Removed {total_removed} documents with invalid price fields")
    else:
        print("✅ No documents found with letters in price fields")
    
    return total_removed

if __name__ == "__main__":
    print("🔧 Starting database maintenance...")
    print("🔍 Scanning for price fields containing letters A-Z...")
    
    removed_count = clean_communitydata()
    
    print(f"\n📊 Maintenance Complete:")
    print(f"   Total documents removed: {removed_count}")
    print("✅ Database maintenance finished successfully")


"""
Stage 2 Data Fetcher Module
Handles MongoDB property data retrieval from Stage 1 results
"""

import os
from typing import Dict
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import logging

class DataFetcher:
    """Fetches property data from Stage 1 MongoDB results"""
    
    def __init__(self):
        self.uri = os.getenv("MONGO_DB_URI")
        if not self.uri:
            raise ValueError("MONGO_DB_URI environment variable is required")
    
    def get_property_data(self) -> Dict[str, Dict]:
        """
        Fetch property data from homepagedata collection.
        
        Input: None
        Output: Dict with format {_id: {"url": str, "listing_id": str}}
        Description: Retrieves _id, listing_id, and URL from Stage 1 results for Stage 2 processing
        """
        try:
            client = MongoClient(self.uri, server_api=ServerApi('1'))
            db = client['newhomesource']
            collection = db['homepagedata']
            
            property_data = {}
            cursor = collection.find({}, {"_id": 1, "listing_id": 1, "property_data.url": 1})
            
            for doc in cursor:
                if "_id" in doc and "property_data" in doc and "url" in doc["property_data"]:
                    property_data[doc["_id"]] = {
                        "url": doc["property_data"]["url"],
                        "listing_id": doc.get("listing_id")
                    }
            
            client.close()
            logging.info(f"✅ Fetched {len(property_data)} property records from Stage 1")
            return property_data
            
        except Exception as e:
            logging.error(f"❌ Error fetching property data: {e}")
            return {}

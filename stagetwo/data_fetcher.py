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
        Output: Dict with format {_id: {"url": str, "listing_id": str, "county": str, "addressLocality": str, "postalCode": str}}
        Description: Retrieves _id, listing_id, URL, and location data from Stage 1 results for Stage 2 processing
        """
        try:
            client = MongoClient(self.uri, server_api=ServerApi('1'))
            db = client['newhomesource']
            collection = db['homepagedata']
            
            property_data = {}
            cursor = collection.find({}, {
                "_id": 1, 
                "listing_id": 1, 
                "property_data.url": 1,
                "property_data.address.county": 1,
                "property_data.address.addressLocality": 1,
                "property_data.address.postalCode": 1,
                "property_data.Address.county": 1,
                "property_data.Address.addressLocality": 1,
                "property_data.Address.postalCode": 1,
                "property_data.offers.offeredBy": 1,
                "property_data.accommodationCategory": 1
            })
            
            for doc in cursor:
                if "_id" in doc and "property_data" in doc and "url" in doc["property_data"]:
                    # Handle both "Address" (capital) and "address" (lowercase) field names
                    property_data_obj = doc.get("property_data", {})
                    address_data = property_data_obj.get("Address") or property_data_obj.get("address", {})
                    
                    property_data[doc["_id"]] = {
                        "url": property_data_obj["url"],
                        "listing_id": doc.get("listing_id"),
                        "county": address_data.get("county"),
                        "addressLocality": address_data.get("addressLocality"),
                        "postalCode": address_data.get("postalCode"),
                        "offeredBy": property_data_obj.get("offers", {}).get("offeredBy"),
                        "accommodationCategory": property_data_obj.get("accommodationCategory")
                    }
            
            client.close()
            logging.info(f"✅ Fetched {len(property_data)} property records from Stage 1")
            return property_data
            
        except Exception as e:
            logging.error(f"❌ Error fetching property data: {e}")
            return {}
    
    def get_homepage_data(self, homepage_id: str) -> Dict:
        """
        Fetch a single homepage document by ID.
        
        Input: homepage_id (str) - the _id of the homepage document
        Output: Dict with the homepage document or empty dict if not found
        Description: Retrieves a single homepage document for retry operations
        """
        try:
            client = MongoClient(self.uri, server_api=ServerApi('1'))
            db = client['newhomesource']
            collection = db['homepagedata']
            
            # Return the full document so nested fields can be accessed
            doc = collection.find_one({"_id": homepage_id})
            client.close()
            
            if doc:
                return doc
            else:
                logging.warning(f"⚠️ No homepage data found for ID: {homepage_id}")
                return {}
                
        except Exception as e:
            logging.error(f"❌ Error fetching homepage data for {homepage_id}: {e}")
            return {}

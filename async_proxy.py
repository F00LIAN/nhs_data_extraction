import asyncio
import curl_cffi
from bs4 import BeautifulSoup
import os
import json
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import time
from typing import Dict, List, Tuple
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

load_dotenv()

def get_property_urls_from_mongodb():
    """Fetch property_id and _id pairs from the homepagedata collection."""
    try:
        uri = os.getenv("MONGO_DB_URI")
        client = MongoClient(uri, server_api=ServerApi('1'))
        db = client['newhomesource']
        collection = db['homepagedata']
        
        property_urls = {}
        cursor = collection.find({}, {"_id": 1, "property_id": 1})
        for doc in cursor:
            if "_id" in doc and doc["_id"]:
                property_urls[doc["_id"]] = doc["property_id"]
        
        client.close()
        return property_urls
    except Exception as e:
        print(f"Error fetching URLs: {e}")
        return {}

california_rotating_proxy = {
    'https': 'http://35d146gk4kq9otn-country-us-state-california:z51vlpkz84emlb9@rp.scrapegw.com:6060'
}

class AsyncProxyHandler:
    def __init__(self, max_concurrent=10, delay_between_requests=1.0, max_retries=3):
        self.max_concurrent = max_concurrent
        self.delay_between_requests = delay_between_requests
        self.max_retries = max_retries
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
    async def connect_to_mongodb(self):
        """Async MongoDB connection"""
        try:
            uri = os.getenv("MONGO_DB_URI")
            client = AsyncIOMotorClient(uri)
            db = client['newhomesource']
            collection = db['homepagedata']
            temp_collection = db['temphtml']
            return client, collection, temp_collection
        except Exception as e:
            raise Exception(f"Failed to connect to MongoDB: {e}")

    async def fetch_with_proxy(self, url: str, impersonation: str = "chrome", max_retries: int = 3) -> Tuple[int, str]:
        """Async HTTP request using curl_cffi with exponential backoff retry"""
        async with self.semaphore:  # Limit concurrent requests
            last_exception = None
            
            for attempt in range(max_retries + 1):  # +1 for initial attempt
                try:
                    # Add delay between requests for rate limiting
                    if attempt > 0:
                        # Exponential backoff: 2^attempt seconds (2, 4, 8...)
                        backoff_delay = min(2 ** attempt, 30)  # Cap at 30 seconds
                        logging.info(f"🔄 Retry {attempt} for {url}, waiting {backoff_delay}s...")
                        await asyncio.sleep(backoff_delay)
                    else:
                        await asyncio.sleep(self.delay_between_requests)
                    
                    # Use curl_cffi.get with await to make it async-compatible
                    response = await asyncio.to_thread(
                        curl_cffi.get,
                        url,
                        impersonate=impersonation,
                        proxies=california_rotating_proxy,
                        timeout=30
                    )
                    
                    # Success for any 2xx status code
                    if 200 <= response.status_code < 300:
                        if attempt > 0:
                            logging.info(f"✅ Request succeeded on attempt {attempt + 1} for {url}")
                        return response.status_code, response.text
                    
                    # Handle specific error codes
                    elif response.status_code in [403, 429, 500, 502, 503, 504]:
                        logging.warning(f"⚠️ HTTP {response.status_code} for {url} (attempt {attempt + 1})")
                        if attempt == max_retries:
                            return response.status_code, ""
                        continue
                    else:
                        # Don't retry for other 4xx errors (404, 400, etc.)
                        logging.error(f"❌ HTTP {response.status_code} error for {url} - not retrying")
                        return response.status_code, ""
                        
                except Exception as e:
                    last_exception = e
                    logging.warning(f"⚠️ Request exception for {url} (attempt {attempt + 1}): {e}")
                    if attempt == max_retries:
                        break
            
            # All retries failed
            logging.error(f"❌ All {max_retries + 1} attempts failed for {url}")
            if last_exception:
                logging.error(f"❌ Last exception: {last_exception}")
            return 0, ""

    async def process_single_url(self, temp_collection, _id: str, url: str,
                               impersonation: str = "chrome") -> bool:
        """Process a single URL async with retry logic using curl_cffi"""
        status_code, html = await self.fetch_with_proxy(url, impersonation, self.max_retries)
        
        if status_code == 200 and html:
            try:
                await temp_collection.insert_one({"_id": _id, "html": html})
                logging.info(f"✅ Successfully saved {url}")
                return True
            except Exception as e:
                logging.error(f"DB insert failed for {_id}: {e}")
                return False
        else:
            logging.warning(f"❌ Failed to fetch {url}, final status: {status_code}")
            return False

    async def retry_with_different_impersonation(self, temp_collection, _id: str, url: str) -> bool:
        """Try different browser impersonations for failed requests using curl_cffi"""
        impersonations = ["safari", "firefox", "chrome_android", "safari_ios"]
        
        for impersonation in impersonations:
            logging.info(f"🔄 Retrying {url} with {impersonation}")
            success = await self.process_single_url(temp_collection, _id, url, impersonation)
            if success:
                return True
            await asyncio.sleep(2)  # Brief delay between retries
        
        return False

    async def reach_target_property_urls_async(self, property_urls: Dict[str, str]):
        """Main async function to process all URLs and extract data"""
        client, homepagedata_collection, temp_collection = await self.connect_to_mongodb()
        
        try:
            # Clear temp collection before starting
            await temp_collection.delete_many({})
            logging.info("🧹 Cleared temp collection")
            
            # Create tasks for all URLs using curl_cffi
            tasks = []
            failed_urls = []
            
            for _id, url in property_urls.items():
                task = self.process_single_url(temp_collection, _id, url)
                tasks.append((_id, url, task))
            
            # Process all URLs concurrently
            logging.info(f"🚀 Processing {len(tasks)} URLs concurrently...")
            start_time = time.time()
            
            for _id, url, task in tasks:
                try:
                    success = await task
                    if not success:
                        failed_urls.append((_id, url))
                except Exception as e:
                    logging.error(f"Task failed for {url}: {e}")
                    failed_urls.append((_id, url))
            
            # Retry failed URLs with different impersonations
            if failed_urls:
                logging.info(f"🔄 Retrying {len(failed_urls)} failed URLs...")
                retry_tasks = []
                
                for _id, url in failed_urls:
                    retry_task = self.retry_with_different_impersonation(temp_collection, _id, url)
                    retry_tasks.append(retry_task)
                
                await asyncio.gather(*retry_tasks, return_exceptions=True)
            
            end_time = time.time()
            logging.info(f"✅ Completed HTML fetching in {end_time - start_time:.2f} seconds")
            
            # Now extract data from temp collection
            await self._extract_from_temp_collection(homepagedata_collection, temp_collection)
            
        finally:
            client.close()

    async def extract_community_info_async(self, html: str, _id: str) -> Dict:
        """Async version of HTML extraction (CPU-bound, but can be made async)"""
        # Use asyncio.to_thread for CPU-bound operations in Python 3.9+
        return await asyncio.to_thread(self._extract_sync, html, _id)
    
    def _extract_sync(self, html: str, _id: str) -> Dict:
        """Extract data from multiple housing cards on the page"""
        extracted_info = {"_id": _id, "properties": []}
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find all housing card containers
        housing_cards = soup.find_all("div", class_="nhs-c-card--housing")
        
        for card_index, card in enumerate(housing_cards):
            property_data = {}
            
            # Extract build statuses from this card
            status_div = card.find("div", class_="nhs-c-card__statuses")
            if status_div:
                build_statuses = []
                for span in status_div.find_all("span"):
                    text = span.get_text(strip=True)
                    if text:
                        build_statuses.append(text)
                if build_statuses:
                    property_data["build_status"] = build_statuses
            
            # Extract JSON-LD scripts from this card
            scripts = card.find_all("script", type="application/ld+json")
            for script in scripts:
                try:
                    data = json.loads(script.string)
                    if data.get("@type") == "SingleFamilyResidence":
                        property_data.update({
                            "name": data.get("name"),
                            "url": data.get("url"),
                            "image": data.get("image"),
                            "build_type": self._determine_build_type(data.get("url", ""))
                        })
                    elif data.get("@type") == "Product":
                        offers = data.get("offers", {})
                        property_data.update({
                            "price": offers.get("price"),
                            "price_currency": offers.get("priceCurrency")
                        })
                except (json.JSONDecodeError, AttributeError):
                    continue
            
            # Add property to list if we found any data
            if property_data:
                property_data["card_index"] = card_index
                extracted_info["properties"].append(property_data)
        
        # Add summary info
        extracted_info["total_properties_found"] = len(extracted_info["properties"])
        
        return extracted_info
    
    def _determine_build_type(self, url: str) -> str:
        """Determine build type from URL"""
        if "spec" in url.lower():
            return "spec"
        elif "plan" in url.lower():
            return "plan"
        return "unknown"

    async def _extract_from_temp_collection(self, homepagedata_collection, temp_collection):
        """Extract data from temp collection and save to main collection"""
        try:
            # Fetch all documents from temp collection
            cursor = temp_collection.find({})
            documents = await cursor.to_list(length=None)
            
            if not documents:
                logging.warning("⚠️ No HTML documents found in temp collection")
                return
            
            # Create extraction tasks
            extraction_tasks = []
            for doc in documents:
                _id = doc["_id"]
                html = doc["html"]
                task = self.extract_community_info_async(html, _id)
                extraction_tasks.append((_id, task))
            
            # Process all extractions concurrently
            logging.info(f"🔍 Extracting data from {len(extraction_tasks)} HTML documents...")
            
            successful_extractions = 0
            for _id, task in extraction_tasks:
                try:
                    extracted_info = await task
                    # Save extracted data back to homepagedata collection with same _id
                    await homepagedata_collection.update_one(
                        {"_id": _id}, 
                        {"$set": {"extracted_info": extracted_info}}, 
                        upsert=True  # Prevents duplicate _id errors
                    )
                    successful_extractions += 1
                    logging.info(f"✅ Extracted and saved data for {_id}")
                except Exception as e:
                    logging.error(f"❌ Extraction failed for {_id}: {e}")
            
            logging.info(f"✅ Successfully extracted {successful_extractions}/{len(documents)} documents")
            
        finally:
            # Always clean up temp collection
            deleted_count = await temp_collection.delete_many({})
            logging.info(f"🧹 Cleaned up {deleted_count.deleted_count} temp documents")

# Usage example
async def main():
    """Main async execution"""
    logging.basicConfig(level=logging.INFO)
    
    # Initialize handler with concurrency settings
    handler = AsyncProxyHandler(
        max_concurrent=10,  # Process 10 URLs simultaneously
        delay_between_requests=0.5,  # 500ms between requests
        max_retries=3  # Retry failed requests up to 3 times
    )
    
    property_urls = get_property_urls_from_mongodb()

    # Process URLs and extract data (all in one flow)
    await handler.reach_target_property_urls_async(property_urls)

if __name__ == "__main__":
    asyncio.run(main())

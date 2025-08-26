import requests
import httpx
from bs4 import BeautifulSoup
import os
import tenacity
import curl_cffi
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import logging
import json
import pprint

california_rotating_proxy = {
    'https': 'http://35d146gk4kq9otn-country-us-state-california:z51vlpkz84emlb9@rp.scrapegw.com:6060'
}

load_dotenv()

def check_status_code(status_code):
    if status_code != 200:
        print("Bad status code")
        print(status_code)
        return False
    else:
        print(f"Good status code {status_code}")
        return True
    
def connect_to_mongodb():
    """Connect to MongoDB and return collection."""
    try:  # get env variable from .env file
        uri = os.getenv("MONGO_DB_URI")
        client = MongoClient(uri, server_api=ServerApi('1'))
        client.admin.command('ping')
        
        db = client['newhomesource']
        collection = db['homepagedata']
        temp_collection = db['temphtml']
        
        return client, collection, temp_collection
        
    except Exception as e:
        raise Exception(f"Failed to connect to MongoDB: {e}")

def get_property_urls_from_mongo():
    """
    Fetch property_id and _id pairs from the homepagedata collection.
    """
    client, collection, _ = connect_to_mongodb()
    print("✅ Successfully connected to MongoDB! Fetching property urls..")

    # Fetch documents with both _id and property_id, save to a dictionary
    property_urls = {}
    cursor = collection.find({}, {"_id": 1, "property_id": 1})
    for doc in cursor:
        if "_id" in doc:
            if doc["_id"]:
                property_urls[doc["_id"]] = doc["property_id"]

    return property_urls

#property_urls = get_property_urls_from_mongo()
#pprint.pprint(property_urls.keys())

def reach_target_property_url_with_proxy(property_urls, impersonation, proxy):
    """
    Takes in a dictionary of _id and property_id pairs.
    For each _id, reach the target property url and check the status code.
    If status code is 200, save the html to temp collection in mongodb. 
    If status code is not 200, create a queue to try different approaches. 
    """
    client, _, temp_collection = connect_to_mongodb()
    try:
        for _id, property_id in property_urls.items():
            url = property_id
            r = curl_cffi.get(url, impersonate=impersonation, proxies=proxy)
            if r.status_code == 200 and r.text:
                temp_collection.insert_one({"_id": _id, "html": r.text})
            else:
                print(f"Bad status code {r.status_code} for {url}")
    except Exception as e:
        print(f"Error in reach_target_property_url_with_proxy: {e}")
    finally:
        client.close()

def reach_target_property_url(property_urls, impersonation="chrome", proxy=california_rotating_proxy):
    """
    Takes in a dictionary of _id and property_id pairs.
    For each _id, reach the target property url and check the status code.
    If status code is 200, save the html to temp collection in mongodb. 
    If status code is not 200, create a queue to try different approaches. 
    """
    client, _, temp_collection = connect_to_mongodb()
    for _id, property_id in property_urls.items():
        bad_status_code_queue = []
        url = property_id
        r = curl_cffi.get(url, impersonate=impersonation, proxies=proxy)
        # Check if html is not empty, just in case
        if r.status_code == 200 and r.text:
            #print(r.text)
            temp_collection.insert_one({"_id": _id, "html": r.text})
        else:
            print(f"Bad status code {r.status_code} for {url}. Adding to queue..")
            bad_status_code_queue = handle_bad_status_code(_id, property_id, bad_status_code_queue)
            rotate_impersonation(bad_status_code_queue)

def handle_bad_status_code(_id, property_id, bad_status_code_queue):
    """
    Takes in the status code, _id, and property_id.
    If the corresponding status code is not 200, add it to the queue.
    """
    bad_status_code_queue.append({"_id": _id, "property_id": property_id})

    return bad_status_code_queue

#@tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(multiplier=1, min=4, max=10))
def rotate_impersonation(bad_status_code_queue, max_attempts=3):
    """
    Helper function to rotate the impersonation. 
    These would interact with the queue of bad status codes and will retry.
    """
    impersonation_list = ["safari", "chrome", "firefox", "chrome_android", "safari_ios"]
    for attempt, impersonation in enumerate(impersonation_list):
        if attempt >= max_attempts:
            break
        print(f"Rotating impersonation to {impersonation}...")
        # Process queue items with new impersonation
        for item in bad_status_code_queue:
            url = item["property_id"]
            try:
                r = curl_cffi.get(url, impersonate=impersonation, proxies=california_rotating_proxy)
                if r.status_code == 200:
                    print(f"Success with {impersonation} for {url}")
                    return True
            except Exception as e:
                print(f"Failed with {impersonation}: {e}")
    return False

def execute_extraction_html():
    """
    Connect to mongodb and get the temphtml collection.
    For each document in the temphtml collection, exract key informatiom from the html. 
    Save the extracted information to the homepagedata collection with the associated _id.
    Clean up the temphtml collection..
    """
    client, homepagedata_collection, temp_collection = connect_to_mongodb()
    cursor = temp_collection.find({})
    for doc in cursor:
        _id = doc["_id"]
        html = doc["html"]
        extracted_info = extract_community_info_from_html(_id, html)
        #homepagedata_collection.update_one({"_id": _id}, {"$set": {"extracted_info": extracted_info}})
        #pprint.pprint(extracted_info)
        print(extracted_info)   # TODO: remove this after testing. 

    temp_collection.delete_many({})
    client.close()
    print("✅ Successfully extracted and cleaned up the temphtml collection!")
    
def extract_community_info_from_html(_id, html):
    """
    Extract key information from the html. Key Locations:
    - <div> with class name "nhs-c-card__statuses". Under <span> is the text.
    - All <script> tags with type "application/ld+json". specifically look for @type = SingleFamilyResidence and Product.
    - Classify the build type based on the url.

    Save to html with the associated _id.
    """
    extracted_info = {}
    soup = BeautifulSoup(html, 'html.parser')
    status_div = soup.find("div", class_="nhs-c-card__statuses")

    # Append ID to extracted_info for good housekeeping and for reinsertion to the homepagedata collection
    extracted_info["_id"] = _id
    
    if status_div:
        for span in status_div.find_all("span"):
            if span.get_text(strip=True):
                extracted_info["build_status"] = span.get_text(strip=True)

    scripts = soup.find_all("script", type="application/ld+json")
    for script in scripts:
        try:
            data = json.loads(script.string)
        except (json.JSONDecodeError, AttributeError):
            continue
        if data.get("@type") == "SingleFamilyResidence":
            # do not save as tuple
            extracted_info["name"] = data.get("name")
            extracted_info["url"] = data.get("url")
            extracted_info["image"] = data.get("image")
            extracted_info["build_type"] = determine_build_type(data.get("url"))
        
        elif data.get("@type") == "Product":
            extracted_info["price"] = data.get("offers", {}).get("price")
            extracted_info["price_currency"] = data.get("offers", {}).get("priceCurrency")
            #extracted_info["@type"] = data.get("@type")
            #extracted_info["product_data"] = data
        
    return extracted_info

def determine_build_type(url: str) -> str:
    """
    Determine the build type based on the url.
    If spec is present, return "spec".
    If plan is present, return "plan".
    Else, return "unknown".
    """
    if "spec" in url:
        return "spec"
    elif "plan" in url:
        return "plan"
    else:
        return "unknown"

if __name__ == "__main__":
    property_urls = get_property_urls_from_mongo()
    #property_urls = {
    #    "1": "https://www.newhomesource.com/community/ca/perris/rockridge-by-kb-home/202198",
    #    "2": "https://www.newhomesource.com/community/ca/winchester/oliva-at-siena-by-taylor-morrison/200717",
    #    "3": "https://www.newhomesource.com/community/ca/peis/rockrge-by-kb-home/02198" #404
    #}
    reach_target_property_url(property_urls, impersonation="chrome", proxy=california_rotating_proxy)
    execute_extraction_html()

 
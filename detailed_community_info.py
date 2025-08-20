import curl_cffi
from bs4 import BeautifulSoup
import json
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime
import os
from datetime import datetime
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import hashlib
import logging
from dotenv import load_dotenv

load_dotenv()

class BuildTypeClassifier:
    """Handles classification of build types based on URL patterns."""
    
    @staticmethod
    def determine_build_type(url: str) -> str:
        """
        Determine if a property is a spec home or plan home based on URL pattern.
        
        Args:
            url: Property URL to analyze
            
        Returns:
            String indicating build type: "spec", "plan", or "unknown"
        """
        if not url or not isinstance(url, str):
            return "unknown"
        
        url_lower = url.lower()
        
        # Check for spec home pattern
        if "/specdetail/" in url_lower:
            return "spec"
        
        # Check for plan home pattern
        elif "/plan/" in url_lower:
            return "plan"
        
        # Check for other possible patterns
        elif "/floorplan/" in url_lower:
            return "plan"
        
        # If no pattern matches
        else:
            return "unknown"
    
    @staticmethod
    def get_build_type_description(build_type: str) -> str:
        """
        Get human-readable description of build type.
        
        Args:
            build_type: Build type code ("spec", "plan", or "unknown")
            
        Returns:
            Human-readable description
        """
        descriptions = {
            "spec": "Spec Home - Move-in ready or under construction",
            "plan": "Plan Home - To be built/customizable floor plan",
            "unknown": "Unknown - Unable to determine build type"
        }
        return descriptions.get(build_type, descriptions["unknown"])

class PropertyDataExtractor:
    """Handles extraction and parsing of property data from JSON-LD schemas."""
    
    @staticmethod
    def extract_property_data(div_element: Any) -> Optional[Dict[str, Any]]:
        """
        Extract property data from a housing div element.
        
        Args:
            div_element: BeautifulSoup div element containing property data
            
        Returns:
            Dictionary containing extracted property data or None if no data found
        """
        property_data = {}
        
        # Find all script tags with type application/ld+json
        scripts = div_element.find_all("script", type="application/ld+json")
        
        for script in scripts:
            try:
                data = json.loads(script.string)
                
                # Extract SingleFamilyResidence data
                if data.get("@type") == "SingleFamilyResidence":
                    property_data.update(
                        PropertyDataExtractor._extract_residence_data(data)
                    )
                
                # Extract Product/pricing data
                elif data.get("@type") == "Product":
                    property_data.update(
                        PropertyDataExtractor._extract_product_data(data)
                    )
                    
            except (json.JSONDecodeError, AttributeError) as e:
                logging.warning(f"Failed to parse JSON-LD script: {e}")
                continue
        
        # Add build type classification if we have a URL
        if property_data and property_data.get("url"):
            url = property_data["url"]
            build_type = BuildTypeClassifier.determine_build_type(url)
            property_data["build_type"] = build_type
            property_data["build_type_description"] = BuildTypeClassifier.get_build_type_description(build_type)
        
        return property_data if property_data else None
    
    @staticmethod
    def _extract_residence_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from SingleFamilyResidence schema."""
        return {
            "name": data.get("name"),
            "url": data.get("url"),
            "image": data.get("image"),
            # Uncomment if needed:
            # "telephone": data.get("telephone"),
            # "address": data.get("Address", {}),
            # "geo_coordinates": data.get("Geo", {})
        }
    
    @staticmethod
    def _extract_product_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from Product schema."""
        offers = data.get("offers", {})
        return {
            "price": offers.get("price"),
            "price_currency": offers.get("priceCurrency")
        }

class CommunityDetailsScraper:
    """Handles web scraping of community details from NewHomeSource."""
    
    def __init__(self, browser_impersonation: str = "chrome"):
        """
        Initialize the scraper.
        
        Args:
            browser_impersonation: Browser to impersonate for requests
        """
        self.browser_impersonation = browser_impersonation
        self.session = None
        self.data_extractor = PropertyDataExtractor()
    
    def __enter__(self):
        """Context manager entry - create session."""
        self.session = curl_cffi.Session(impersonate=self.browser_impersonation)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup session."""
        if self.session:
            self.session.close()
    
    def scrape_community_url(self, url: str) -> List[Dict[str, Any]]:
        """
        Scrape property details from a community URL.
        
        Args:
            url: Community URL to scrape
            
        Returns:
            List of property data dictionaries
            
        Raises:
            Exception: If scraping fails
        """
        if not self.session:
            raise RuntimeError("Scraper must be used as context manager")
        
        try:
            # Fetch the page
            response = self.session.get(url, impersonate=self.browser_impersonation)
            response.raise_for_status()
            
            # Parse HTML
            soup = BeautifulSoup(response.text, "html.parser")
            housing_divs = soup.find_all("div", class_="nhs-c-card--housing")
            
            if not housing_divs:
                logging.warning(f"No housing cards found for URL: {url}")
                return []
            
            # Extract property data from each div
            results = []
            for div in housing_divs:
                property_data = self.data_extractor.extract_property_data(div)
                if property_data:
                    results.append(property_data)
            
            logging.info(f"Successfully extracted {len(results)} properties from {url}")
            return results
            
        except Exception as e:
            logging.error(f"Failed to scrape {url}: {e}")
            raise

def scrape_single_community(url: str, browser_impersonation: str = "chrome") -> List[Dict[str, Any]]:
    """
    Convenience function to scrape a single community URL.
    
    Args:
        url: Community URL to scrape
        browser_impersonation: Browser to impersonate
        
    Returns:
        List of property data dictionaries
    """
    with CommunityDetailsScraper(browser_impersonation) as scraper:
        return scraper.scrape_community_url(url)

def update_json_with_community_info():
    """Read document_architecture.json, scrape community_info, and update the file."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    json_file = 'document_architecture.json'
    
    try:
        # Load the JSON document
        with open(json_file, 'r') as f:
            document = json.load(f)
        
        # Get the property_id URL
        property_url = document.get("property_id")
        if not property_url:
            logging.error("No property_id found in document")
            return 1
        
        logging.info(f"Scraping community details from: {property_url}")
        
        # Scrape the community details
        community_properties = scrape_single_community(property_url)
        
        if not community_properties:
            logging.warning("No community properties found")
            return 1
        
        # Update the document
        document["community_info"] = community_properties
        document["community_scraping_metadata"] = {
            "properties_found": len(community_properties),
            "scraping_successful": True,
            "last_community_scrape": datetime.now().isoformat()
        }
        
        # Save back to file
        with open(json_file, 'w') as f:
            json.dump(document, f, indent=2)
        
        logging.info(f"✅ Updated {json_file} with {len(community_properties)} properties")
        print(json.dumps(community_properties, indent=2))
        
        return 0
        
    except Exception as e:
        logging.error(f"Failed to update JSON file: {e}")
        return 1

def main():
    """Main function - updates document_architecture.json with community info."""
    return update_json_with_community_info()

if __name__ == "__main__":
    exit(main())
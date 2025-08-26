import json
import os
from typing import List, Dict, Tuple

class URLGenerator:
    """Generates URLs for scraping based on configuration"""
    
    def __init__(self, config_path: str = None):
        if config_path is None:
            # Look in parent directory (src/scraper) for the config file
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "scraper_config.json")
        
        with open(config_path, 'r') as f:
            self.config = json.load(f)
    
    def generate_urls(self, site: str = "newhomesource") -> List[Tuple[str, Dict]]:
        """
        Generate all URLs for a given site
        
        Returns:
            List of tuples: (url, location_info)
        """
        site_config = self.config[site]
        urls = []
        
        for location in site_config["locations"]:
            location_urls = self._generate_location_urls(site_config, location)
            for url in location_urls:
                urls.append((url, location))
        
        return urls
    
    def _generate_location_urls(self, site_config: Dict, location: Dict) -> List[str]:
        """Generate paginated URLs for a specific location"""
        base_url = site_config["base_url"]
        pagination = site_config["pagination"]
        
        # Build base location URL
        location_url = f"{base_url}/{location['state']}/{location['area_region']}/{location['specific_location']}"
        
        urls = []
        for page in range(pagination["start_page"], pagination["end_page"] + 1):
            page_url = f"{location_url}/{pagination['url_pattern'].format(page=page)}"
            urls.append(page_url)
        
        return urls
    
    def get_request_settings(self, site: str = "newhomesource") -> Dict:
        """Get request settings for a site"""
        return self.config[site]["request_settings"]
    
    def add_location(self, site: str, state: str, area_region: str, specific_location: str, display_name: str):
        """Add a new location to the configuration"""
        new_location = {
            "state": state,
            "area_region": area_region, 
            "specific_location": specific_location,
            "display_name": display_name
        }
        self.config[site]["locations"].append(new_location)
    
    def save_config(self, config_path: str = None):
        """Save current configuration to file"""
        if config_path is None:
            # Look in parent directory (src/scraper) for the config file
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "scraper_config.json")
        
        with open(config_path, 'w') as f:
            json.dump(self.config, f, indent=2)

def Generate_URLs():
    """
    Generate URLs for the scraper to scrape.
    """
    url_generator = URLGenerator()
    urls_to_scrape = url_generator.generate_urls()
    request_settings = url_generator.get_request_settings()
    return urls_to_scrape, request_settings
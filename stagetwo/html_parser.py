"""
Stage 2 HTML Parser Module
Extracts community data from property listing HTML pages
"""

import json
import logging
from typing import Dict, List
from bs4 import BeautifulSoup


class HTMLParser:
    """Parses HTML to extract community data from property listings"""
    
    def extract_community_data(self, html: str, homepage_id: str) -> Dict:
        """
        Extract community data from HTML content.
        
        Input: html (str), homepage_id (str)
        Output: Dict with format {"homepage_id": str, "communities": List[Dict], "total_communities_found": int}
        Description: Parses housing cards from HTML and extracts JSON-LD data for each community
        """
        extracted_info = {"homepage_id": homepage_id, "communities": []}
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find housing card containers with multiple selector fallbacks
        housing_cards = self._find_housing_cards(soup)
        logging.debug(f"Found {len(housing_cards)} housing cards in HTML")
        
        for card_index, card in enumerate(housing_cards):
            community_data = self._extract_card_data(card, card_index)
            
            # Add community if we have required data
            if community_data.get("name") and community_data.get("url"):
                community_id = self._generate_community_id(community_data["name"], community_data["url"])
                if community_id:
                    community_data["community_id"] = community_id
                    community_data["card_index"] = card_index
                    extracted_info["communities"].append(community_data)
        
        extracted_info["total_communities_found"] = len(extracted_info["communities"])
        
        # Debug logging for empty results
        if not extracted_info["communities"]:
            logging.warning(f"No communities extracted from {len(housing_cards)} housing cards")
            self._debug_html_structure(soup)
        
        return extracted_info
    
    def _find_housing_cards(self, soup: BeautifulSoup) -> List:
        """Find housing card elements with multiple selector strategies"""
        # Try primary selector
        housing_cards = soup.find_all("div", class_="nhs-c-card--housing")
        if housing_cards:
            return housing_cards
        
        # Fallback selectors
        selectors = [
            lambda s: s.find_all("div", class_="nhs-c-card-housing"),
            lambda s: s.find_all("div", class_=lambda x: x and "nhs-c-card" in x and "housing" in x)
        ]
        
        for selector in selectors:
            housing_cards = selector(soup)
            if housing_cards:
                return housing_cards
        
        return []
    
    def _extract_card_data(self, card, card_index: int) -> Dict:
        """Extract data from a single housing card"""
        community_data = {}
        
        # Extract build statuses
        status_div = card.find("div", class_="nhs-c-card__statuses")
        if status_div:
            build_statuses = [span.get_text(strip=True) for span in status_div.find_all("span") if span.get_text(strip=True)]
            if build_statuses:
                community_data["build_status"] = build_statuses
        
        # Extract JSON-LD data
        scripts = card.find_all("script", type="application/ld+json")
        if scripts:
            self._parse_json_ld_scripts(scripts, community_data)
        else:
            # Fallback parsing when no JSON-LD found
            fallback_data = self._fallback_parse_card(card)
            if fallback_data:
                community_data.update(fallback_data)
        
        return community_data
    
    def _parse_json_ld_scripts(self, scripts: List, community_data: Dict):
        """Parse JSON-LD scripts from housing card"""
        for script in scripts:
            try:
                if script.string:
                    data = json.loads(script.string.strip())
                    
                    if data.get("@type") == "SingleFamilyResidence":
                        community_data.update({
                            "name": data.get("name"),
                            "url": data.get("url"),
                            "image": data.get("image"),
                            "build_type": self._determine_build_type(data.get("url", ""))
                        })
                    elif data.get("@type") == "Product":
                        offers = data.get("offers", {})
                        community_data.update({
                            "price": offers.get("price"),
                            "price_currency": offers.get("priceCurrency")
                        })
            except (json.JSONDecodeError, AttributeError, TypeError) as e:
                logging.debug(f"Failed to parse JSON-LD: {e}")
                continue
    
    def _determine_build_type(self, url: str) -> str:
        """
        Determine build type from URL.
        
        Input: url (str)
        Output: str ("spec", "plan", or "unknown")
        Description: Classifies property type based on URL patterns
        """
        url_lower = url.lower()
        if "spec" in url_lower:
            return "spec"
        elif "plan" in url_lower:
            return "plan"
        return "unknown"
    
    def _generate_community_id(self, name: str, url: str) -> str:
        """
        Generate unique community ID.
        
        Input: name (str), url (str)
        Output: str - unique identifier combining URL and cleaned name
        Description: Creates consistent ID for community tracking across scrapes
        """
        if not name or not url:
            return None
        
        # Clean name for ID format
        clean_name = name.replace(" ", "_").replace(",", "").replace(".", "")
        return f"{url}_{clean_name}"
    
    def _fallback_parse_card(self, card_element):
        """
        Fallback parsing when JSON-LD scripts are not available.
        
        Input: card_element (BeautifulSoup element)
        Output: Dict or None - extracted data or None if parsing fails
        Description: Alternative parsing method for cards without structured data
        """
        # TODO: Implement fallback parsing patterns based on HTML structure
        logging.debug("Fallback parsing needed - implement pattern identification")
        return None
    
    def _debug_html_structure(self, soup: BeautifulSoup):
        """Log HTML structure for debugging empty results"""
        all_divs = soup.find_all("div", class_=True)[:10]
        logging.debug(f"Sample div classes: {[div.get('class') for div in all_divs]}")

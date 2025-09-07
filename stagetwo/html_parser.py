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

    def __init__(self):
        # Define all selectors in one place - easy to add new ones
        self.card_selectors = [
            # Primary selectors (most common)
            lambda s: s.find_all("div", class_="nhs-c-card--housing"),
            lambda s: s.find_all("div", class_="nhs-c-card-housing"),
            
            # New website style selectors
            lambda s: s.find_all("div", class_="card__home-item clearfix"),
            lambda s: s.find_all("div", class_="card__home-item"),  # More flexible
            
            # Pattern-based selectors
            lambda s: s.find_all("div", class_=lambda x: x and "nhs-c-card" in x and "housing" in x),
            lambda s: s.find_all("div", class_=lambda x: x and "card" in x and "home" in x),

            lambda s: s.find_all("div", class_="card-listing__info"),
            lambda s: s.find_all("div", {"data-listing-info": True}),
            lambda s: s.find_all("div", class_=lambda x: x and "card-listing" in x),
            lambda s: s.find_all("div", class_=lambda x: x and "card-listing__info" in x),
            lambda s: s.find_all("div", class_=lambda x: x and "card-listing__info" in x),
            
            # Add new selectors here as needed:
            # lambda s: s.find_all("div", class_="new-website-style"),
            # lambda s: s.find_all("article", class_="property-card"),
        ]
        
        # Status extraction selectors - multiple fallbacks for different HTML structures
        self.status_selectors = [
            lambda s: s.find("div", class_="nhs-c-card__statuses"),
            lambda s: s.find("p", class_="label--purple listing__label"),
            lambda s: s.find("span", class_="status-label"),
            lambda s: s.find("div", class_=lambda x: x and "status" in x),
            lambda s: s.find("span", class_="listing__label"),
            lambda s: s.find("span", {"data-qa": "listing-status"}),
            # Add new status selectors here as needed
        ]
        
        # JSON-LD script selectors - multiple types of structured data
        self.json_ld_selectors = [
            lambda s: s.find_all("script", type="application/ld+json"),
            lambda s: s.find_all("script", type="application/json"),
            lambda s: s.find_all("script", {"data-type": "structured-data"}),
            # Add new JSON-LD selectors here as needed
        ]
        
        # Fallback parsing selectors for when JSON-LD is not available
        self.fallback_selectors = [
            lambda s: s.find("h3", class_="community-name"),
            lambda s: s.find("a", class_="community-link"),
            lambda s: s.find("div", class_=lambda x: x and "name" in x),
            # Add new fallback selectors here as needed
        ]
        
        # HTML Fallback Selectors - for extracting data when JSON-LD is missing
        # These are organized by data type for easy maintenance and extension
        self.html_fallback_selectors = {
            "name": [
                lambda s: s.find("p", class_="home__item-name"),
                lambda s: s.find("h3", class_="community-name"),
                lambda s: s.find("div", class_=lambda x: x and "name" in x),
                lambda s: s.find("span", class_="property-title"),
                # Add new name selectors here as needed
                lambda s: s.find("p", class_="info__name"),
                lambda s: s.find("a", {"data-home-link": True}),    
            ],
            "url": [
                lambda s: s.find("p", class_="home__item-name").find("a") if s.find("p", class_="home__item-name") else None,
                lambda s: s.find("a", class_="community-link"),
                lambda s: s.find("a", class_="property-link"),
                lambda s: s.find("a", href=True),
                # Add new URL selectors here as needed
            ],
            "image": [
                lambda s: s.find("picture").find("img") if s.find("picture") else None,
                lambda s: s.find("img", class_="property-image"),
                lambda s: s.find("img", class_=lambda x: x and "image" in x),
                lambda s: s.find("img", src=True),
                # Add new image selectors here as needed
            ],
            "price": [
                lambda s: s.find("span", {"data-card-element": "Price", "data-qa": "price_label"}),  # Edge case price labels
                lambda s: s.find("p", class_="home__price"),
                lambda s: s.find("span", class_="price"),
                lambda s: s.find("div", class_=lambda x: x and "price" in x),
                lambda s: s.find("span", class_=lambda x: x and "$" in x.get_text() if x else False),
                # Add new price selectors here as needed
                lambda s: s.find("span", class_="info__price-values"),
                lambda s: s.find("p", class_="info__price"),
            ],
            "build_type": [
                lambda s: s.find("span", class_="build-type"),
                lambda s: s.find("div", class_=lambda x: x and "type" in x),
                # Add new build type selectors here as needed
            ]
        }
        
        # Build type patterns for URL analysis
        self.build_type_patterns = [
            "spec", "plan", "quick", "move-in", "available", "basiccommunity"
        ]
        
        # Community ID generation strategies
        self.community_id_strategies = [
            "url_name_combination",  # current approach
        ]
    
    def extract_community_data(self, html: str, homepage_id: str, address_locality: str, county: str, postal_code: str, offered_by: str = None, accommodation_category: str = None) -> Dict:
        """
        Extract community data from HTML content.
        
         Input: html (str), homepage_id (str), address_locality (str), county (str), postal_code (str), offered_by (str), accommodation_category (str)
        Output: Dict with format {"homepage_id": str, "county": str, "address_locality": str, "postal_code": str, "communities": List[Dict], "total_communities_found": int}
        Description: Parses housing cards from HTML and extracts JSON-LD data for each community
        """
        extracted_info = {"homepage_id": homepage_id,
                          "county": county,
                          "address_locality": address_locality,
                          "postal_code": postal_code,
                          "communities": []}
        soup = BeautifulSoup(html, 'html.parser')
        
        # Store county for JSON-LD parsing
        self._current_county = county
        
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
                    # These fields are now mandatory - use fallback if not available
                    community_data["offeredBy"] = offered_by if offered_by else "Unknown"
                    community_data["accommodationCategory"] = accommodation_category if accommodation_category else "Unknown"
                    extracted_info["communities"].append(community_data)
                else:
                    # Create "Coming Soon" entry when community_id generation fails
                    coming_soon_community = {
                        "build_status": ["Coming Soon"],
                        "name": "Coming Soon",
                        "url": "Coming Soon", 
                        "image": "Coming Soon",
                        "address": {
                            "@type": "PostalAddress",
                            "county": county,
                            "addressLocality": address_locality,
                            "postalCode": postal_code
                        },
                        "build_type": "Coming Soon",
                        "price": "Coming Soon",
                        "price_currency": "USD",
                        "community_id": "Coming Soon",
                        "card_index": card_index,
                        # These fields are now mandatory - use fallback if not available
                        "offeredBy": offered_by if offered_by else "Unknown",
                        "accommodationCategory": accommodation_category if accommodation_category else "Unknown"
                    }
                    extracted_info["communities"].append(coming_soon_community)
        
        extracted_info["total_communities_found"] = len(extracted_info["communities"])
        
        # Debug logging for empty results
        if not extracted_info["communities"]:
            logging.warning(f"No communities extracted from {len(housing_cards)} housing cards")
            self._debug_html_structure(soup)
            
            # Add "Coming Soon" community entry
            extracted_info["communities"].append({
                "build_status": ["Coming Soon"],
                "name": "Coming Soon",
                "url": "Coming Soon",
                "image": "Coming Soon",
                "address": {
                    "@type": "PostalAddress",
                    "county": county,
                    "addressLocality": address_locality,
                    "postalCode": postal_code
                },
                "build_type": "Coming Soon",
                "price": "Coming Soon",
                "price_currency": "USD",
                "community_id": "Coming Soon",
                "card_index": 0,
                "offeredBy": offered_by if offered_by else "Unknown",
                "accommodationCategory": accommodation_category if accommodation_category else "Unknown"
            })
            extracted_info["total_communities_found"] = 1
        
        return extracted_info
    
    def _is_valid_edge_case_price(self, price: str) -> bool:
        """
        Check if price text is a valid edge case that should be preserved.
        
        Input: price (str) - price text to validate
        Output: bool - True if valid edge case pricing
        Description: Validates edge case pricing to preserve meaningful text instead of defaulting to "Coming Soon"
        """
        if not isinstance(price, str):
            return False
        
        price = price.strip()
        if not price:
            return False
        
        # Define acceptable edge case pricing values that should be preserved
        edge_cases = {
            "0",
            "Contact Builder for Details", 
            "Coming Soon",
            "Pricing Not Available",
            "No Pricing Available",
            "Price not available",
            "Call for Pricing",
            "See Sales Representative"
        }
        
        # Check if it's a preserved edge case
        if price in edge_cases:
            return True
        
        # Check if it contains meaningful pricing information ($ or digits)
        if "$" in price or any(char.isdigit() for char in price):
            return True
        
        return False
    
    def _has_valid_edge_case_data(self, community_data: dict) -> bool:
        """
        Check if community data has valid edge case pricing that should be preserved.
        
        Input: community_data (dict) - extracted community data
        Output: bool - True if has valid edge case data
        Description: Determines if community should be saved based on edge case pricing
        """
        price = community_data.get("price", "")
        return self._is_valid_edge_case_price(price)
    
    def _find_housing_cards(self, soup: BeautifulSoup) -> List:
        """Find housing card elements with multiple selector strategies"""
        for selector in self.card_selectors:
            try:
                housing_cards = selector(soup)
                if housing_cards:
                    logging.debug(f"Found {len(housing_cards)} cards using selector: {selector.__name__ if hasattr(selector, '__name__') else 'lambda'}")
                    return housing_cards
            except Exception as e:
                logging.debug(f"Selector failed: {e}")
                continue
        
        logging.warning("No housing cards found with any selector")
        return []
    
    def extract_status(self, card: BeautifulSoup) -> List[str]:
        """Extract build statuses from housing card using multiple selector strategies"""
        for selector in self.status_selectors:
            try:
                status_tag = selector(card)
                if status_tag:
                    # Try to extract text from spans within the status tag
                    status_spans = status_tag.find_all("span")
                    if status_spans:
                        build_statuses = [span.get_text(strip=True) for span in status_spans if span.get_text(strip=True)]
                    else:
                        build_statuses = [status_tag.get_text(strip=True)]
                    
                    if build_statuses:
                        logging.debug(f"Extracted statuses: {build_statuses}")
                        return build_statuses
            except Exception as e:
                logging.debug(f"Status selector failed: {e}")
                continue
        
        return []
    
    def _extract_card_data(self, card, card_index: int) -> Dict:
        """Extract data from a single housing card"""
        community_data = {}
        
        # Extract build statuses using modular approach
        build_statuses = self.extract_status(card)
        if build_statuses:
            community_data["build_status"] = build_statuses
        
        # Extract JSON-LD data using modular selectors
        scripts = self._find_json_ld_scripts(card)
        if scripts:
            self._parse_json_ld_scripts(scripts, community_data)
        else:
            # Fallback parsing when no JSON-LD found
            fallback_data = self._fallback_parse_card(card)
            if fallback_data:
                community_data.update(fallback_data)
            
            # Return "Coming Soon" if no data is found and no valid edge case pricing
            if not fallback_data:
                # Check if we extracted any valid edge case pricing from earlier selectors
                extracted_price = community_data.get("price", "")
                if self._is_valid_edge_case_price(extracted_price):
                    # Preserve the edge case pricing, fill in missing fields with "Coming Soon"
                    community_data.update({
                        "name": community_data.get("name", "Coming Soon"),
                        "url": community_data.get("url", "Coming Soon"),
                        "image": community_data.get("image", "Coming Soon"),
                        "build_type": community_data.get("build_type", "Coming Soon"),
                        # price is already set with edge case value
                    })
                else:
                    # No valid data found, use "Coming Soon" for everything
                    community_data = {
                        "name": "Coming Soon",
                        "url": "Coming Soon",
                        "image": "Coming Soon",
                        "build_type": "Coming Soon",
                        "price": "Coming Soon",
                    }
        
        return community_data
    
    def _find_json_ld_scripts(self, card: BeautifulSoup) -> List:
        """Find JSON-LD scripts using multiple selector strategies"""
        for selector in self.json_ld_selectors:
            try:
                scripts = selector(card)
                if scripts:
                    logging.debug(f"Found {len(scripts)} JSON-LD scripts")
                    return scripts
            except Exception as e:
                logging.debug(f"JSON-LD selector failed: {e}")
                continue
        
        return []
    
    def _parse_json_ld_scripts(self, scripts: List, community_data: Dict):
        """Parse JSON-LD scripts from housing card"""
        for script in scripts:
            try:
                if script.string:
                    data = json.loads(script.string.strip())
                    
                    if data.get("@type") == "SingleFamilyResidence":
                        # Get address and add county if available
                        address = data.get("address", {})
                        if isinstance(address, dict) and address:
                            # Add county to address if not present
                            if "county" not in address and hasattr(self, '_current_county'):
                                address["county"] = self._current_county
                        
                        community_data.update({
                            "name": data.get("name"),
                            "url": data.get("url"),
                            "image": data.get("image"),
                            "address": address,
                            "build_type": self._determine_build_type(data.get("url", ""))
                        })
                    elif data.get("@type") == "Product":
                        offers = data.get("offers", {})
                        community_data.update({
                            "price": offers.get("price"),
                            "price_currency": offers.get("priceCurrency") # remove not needed
                        })
            except (json.JSONDecodeError, AttributeError, TypeError) as e:
                logging.debug(f"Failed to parse JSON-LD: {e}")
                continue
    
    def _determine_build_type(self, url: str) -> str:
        """
        Determine build type from URL using modular patterns.
        
        Input: url (str)
        Output: str (build type or "unknown")
        Description: Classifies property type based on URL patterns
        """
        url_lower = url.lower()
        for pattern in self.build_type_patterns:
            if pattern in url_lower:
                return pattern
        return "unknown"
    
    def _generate_community_id(self, name: str, url: str) -> str:
        """
        Generate unique community ID using multiple strategies.
        
        Input: name (str), url (str)
        Output: str - unique identifier
        Description: Creates consistent ID for community tracking across scrapes
        """
        if not name or not url:
            return None
        
        # Use primary strategy: url_name_combination
        try:
            clean_name = name.replace(" ", "_").replace(",", "").replace(".", "")
            return f"{url}_{clean_name}"
        except Exception as e:
            logging.debug(f"Primary ID generation failed: {e}")
            # Could implement fallback strategies here
            return None
    
    def _fallback_parse_card(self, card_element):
        """
        Fallback parsing when JSON-LD scripts are not available.
        
        Input: card_element (BeautifulSoup element)
        Output: Dict or None - extracted data or None if parsing fails
        Description: Alternative parsing method for cards without structured data
        """
        fallback_data = {}
        
        # Try multiple fallback strategies using the organized HTML selectors
        for selector_group_name, selectors in self.html_fallback_selectors.items():
            for selector in selectors:
                try:
                    element = selector(card_element)
                    if element:
                        if selector_group_name == "name":
                            fallback_data["name"] = element.get_text(strip=True)
                        elif selector_group_name == "url":
                            fallback_data["url"] = element.get("href") if element.name == "a" else element.get("src")
                        elif selector_group_name == "image":
                            fallback_data["image"] = element.get("src")
                        elif selector_group_name == "price":
                            fallback_data["price"] = element.get_text(strip=True)
                        elif selector_group_name == "build_type":
                            fallback_data["build_type"] = element.get_text(strip=True)
                        # Add more fallback logic as needed
                except Exception as e:
                    logging.debug(f"Fallback selector failed: {e}")
                    continue
        
        if fallback_data:
            logging.debug(f"Fallback parsing successful: {fallback_data}")
            return fallback_data
        
        logging.debug("Fallback parsing failed - no patterns matched")
        return None
    
    def _debug_html_structure(self, soup: BeautifulSoup):
        """Log HTML structure for debugging empty results"""
        all_divs = soup.find_all("div", class_=True)[:10]
        logging.debug(f"Sample div classes: {[div.get('class') for div in all_divs]}")
        
        # Also log potential card-like elements
        potential_cards = soup.find_all(["div", "article", "section"], class_=True)
        card_classes = [elem.get('class') for elem in potential_cards[:20]]
        logging.debug(f"Potential card elements: {card_classes}")
    
    def add_card_selector(self, selector_func):
        """
        Add a new card selector function.
        
        Input: selector_func (callable) - function that takes soup and returns elements
        Description: Dynamically add new selectors for different HTML structures
        """
        if callable(selector_func):
            self.card_selectors.append(selector_func)
            logging.info(f"Added new card selector: {selector_func.__name__ if hasattr(selector_func, '__name__') else 'lambda'}")
        else:
            logging.error("Selector must be a callable function")
    
    def add_status_selector(self, selector_func):
        """
        Add a new status selector function.
        
        Input: selector_func (callable) - function that takes card element and returns status
        Description: Dynamically add new status extraction methods
        """
        if callable(selector_func):
            self.status_selectors.append(selector_func)
            logging.info(f"Added new status selector: {selector_func.__name__ if hasattr(selector_func, '__name__') else 'lambda'}")
        else:
            logging.error("Selector must be a callable function")
    
    def add_fallback_selector(self, data_type: str, selector_func):
        """
        Add a new fallback selector for a specific data type.
        
        Input: data_type (str) - "name", "url", "image", "price", "build_type"
               selector_func (callable) - function that takes card element and returns data
        Description: Dynamically add new fallback selectors for different HTML structures
        """
        if data_type not in self.html_fallback_selectors:
            logging.error(f"Unknown data type: {data_type}. Valid types: {list(self.html_fallback_selectors.keys())}")
            return
        
        if callable(selector_func):
            self.html_fallback_selectors[data_type].append(selector_func)
            logging.info(f"Added new {data_type} fallback selector: {selector_func.__name__ if hasattr(selector_func, '__name__') else 'lambda'}")
        else:
            logging.error("Selector must be a callable function")
    
    def add_fallback_selectors_batch(self, selectors_dict: dict):
        """
        Add multiple fallback selectors at once.
        
        Input: selectors_dict (dict) - {"data_type": [selector_func1, selector_func2, ...]}
        Description: Bulk add multiple selectors for different data types
        """
        for data_type, selectors in selectors_dict.items():
            if isinstance(selectors, list):
                for selector in selectors:
                    self.add_fallback_selector(data_type, selector)
            else:
                self.add_fallback_selector(data_type, selectors)
    
    def get_fallback_selector_count(self) -> dict:
        """
        Get count of fallback selectors for each data type.
        
        Output: dict - {"data_type": count, ...}
        Description: Useful for debugging and monitoring selector coverage
        """
        return {data_type: len(selectors) for data_type, selectors in self.html_fallback_selectors.items()}
    
    def clear_fallback_selectors(self, data_type: str = None):
        """
        Clear fallback selectors for a specific data type or all types.
        
        Input: data_type (str, optional) - specific data type to clear, or None for all
        Description: Useful for testing or when selectors become outdated
        """
        if data_type:
            if data_type in self.html_fallback_selectors:
                self.html_fallback_selectors[data_type].clear()
                logging.info(f"Cleared all {data_type} fallback selectors")
            else:
                logging.error(f"Unknown data type: {data_type}")
        else:
            for data_type in self.html_fallback_selectors:
                self.html_fallback_selectors[data_type].clear()
            logging.info("Cleared all fallback selectors")

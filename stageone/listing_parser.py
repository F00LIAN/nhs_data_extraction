"""
Listing Parser Module
Handles HTML parsing and JSON-LD extraction for property listings.

Input: HTML content, URL
Output: Structured listing documents
Description: Parse property data from both JSON-LD scripts and HTML fallback methods.
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

class ListingParser:
    def __init__(self):
        pass

    def parse_html_content(self, url: str, html: str, location_info: Dict = None) -> List[Dict]:
        """
        Input: URL string, HTML content string
        Output: List of parsed listing documents
        Description: Main parsing function that extracts all listings from HTML page
        """
        try:
            logging.info(f"🔄 Parsing {url}")
            soup = BeautifulSoup(html, 'html.parser')
            
            nhs_cards = soup.find_all('div', class_='nhs-c-card--housing')
            
            if not nhs_cards:
                logging.warning(f"⚠️ No listing cards found for {url}")
                return []

            extracted_data = []
            
            for card_index, nhs_card in enumerate(nhs_cards):
                try:
                    document = self._parse_single_card(nhs_card, url, card_index, location_info)
                    if document:
                        extracted_data.append(document)
                        logging.info(f"✅ Found listing: {document['listing_id']} (source: {document['data_source']})")
                except Exception as e:
                    logging.warning(f"⚠️ Error parsing card {card_index}: {e}")
                    continue
            
            if extracted_data:
                self._log_found_urls(extracted_data)
                logging.info(f"✅ Parsed {url}: {len(extracted_data)} listings found")
            else:
                logging.warning(f"⚠️ No valid listings found for {url}")
            
            return extracted_data
            
        except Exception as e:
            logging.error(f"❌ Error parsing {url}: {e}")
            return []

    def _parse_single_card(self, nhs_card, source_url: str, card_index: int, location_info: Dict = None) -> Optional[Dict]:
        """
        Input: BeautifulSoup card element, source URL, card index
        Output: Parsed document dict or None
        Description: Parse individual listing card using JSON-LD first, then HTML fallback
        """
        # Try JSON-LD parsing first (preferred method)
        json_ld_data = self._extract_json_ld_data(nhs_card)
        if json_ld_data:
            return self._create_document_from_json_ld(json_ld_data, source_url, location_info)
        
        # Fallback to HTML parsing
        html_data = self._extract_html_data(nhs_card)
        if html_data:
            return self._create_document_from_html(html_data, source_url, location_info)
        
        logging.warning(f"⚠️ No valid data found in card {card_index}")
        return None

    def _extract_json_ld_data(self, nhs_card) -> Optional[Dict]:
        """
        Input: BeautifulSoup card element
        Output: JSON-LD data dict or None
        Description: Extract and validate JSON-LD structured data from card
        """
        scripts = nhs_card.find_all('script', type='application/ld+json')
        
        for script in scripts:
            try:
                data = json.loads(script.text)
                if data.get("url") and data.get("name"):
                    # Handle price fallback for JSON-LD data
                    data = self._handle_json_ld_price_fallback(data, nhs_card)
                    return data
            except json.JSONDecodeError:
                continue
        
        return None

    def _handle_json_ld_price_fallback(self, data: Dict, nhs_card) -> Dict:
        """
        Input: JSON-LD data dict, BeautifulSoup card element
        Output: JSON-LD data dict with updated price if needed
        Description: Handle price fallback for JSON-LD when price is "0"
        """
        try:
            # Check if offers.price is "0"
            offers = data.get("offers", {})
            current_price = offers.get("price", "")
            
            if current_price == "0":
                # Look for price_label span element
                price_span = nhs_card.find('span', {
                    'data-card-element': 'Price',
                    'data-qa': 'price_label'
                })
                
                if price_span:
                    fallback_price = price_span.text.strip()
                    if fallback_price:
                        # Update the price in offers
                        if "offers" not in data:
                            data["offers"] = {}
                        data["offers"]["price"] = fallback_price
                        logging.debug(f"Updated JSON-LD price from '0' to '{fallback_price}'")
                    
        except Exception as e:
            logging.debug(f"Error in JSON-LD price fallback: {e}")
            
        return data

    def _extract_html_data(self, nhs_card) -> Optional[Dict]:
        """
        Input: BeautifulSoup card element
        Output: HTML-extracted data dict or None
        Description: Extract listing data from HTML structure when JSON-LD unavailable
        """
        try:
            price, name, url, address = self._parse_html_card_structure(nhs_card)
            
            if url and name:
                return {
                    "price": price,
                    "name": name, 
                    "url": url,
                    "address": address
                }
        except Exception as e:
            logging.debug(f"HTML parsing failed: {e}")
        
        return None

    def _parse_html_card_structure(self, nhs_card):
        """
        Input: BeautifulSoup card element
        Output: Tuple of (price, name, url, address)
        Description: Parse HTML structure to extract listing details with fallback methods
        """
        try:
            # Find card body and content
            nhs_card_body = nhs_card.find('div', class_='nhs-c-card__body')
            if not nhs_card_body:
                raise ValueError("No nhs-c-card__body found")

            nhs_card_content = nhs_card_body.find('div', class_='nhs-c-card__content')
            if not nhs_card_content:
                raise ValueError("No nhs-c-card__content found")
            
            # Extract price
            price_element = nhs_card_content.find('p', class_='nhs-c-card__price')
            if not price_element:
                raise ValueError("No price element found")
            price = price_element.text.strip()
            
            # Extract name and URL
            name_content = nhs_card_content.find('h3', class_='nhs-c-card__housing-name')
            if not name_content:
                raise ValueError("No housing name element found")

            name = name_content.text.strip()
            
            url_element = name_content.find('a')
            if not url_element or not url_element.get('href'):
                raise ValueError("No URL found in name element")
            url = url_element.get('href')

            # Extract address
            address_element = nhs_card_content.find('p', class_='nhs-c-card__facts', attrs={'data-qa': 'listing_address'})
            if not address_element:
                address_element = nhs_card_content.find('p', class_='nhs-c-card__facts')
                if not address_element:
                    raise ValueError("No address element found")
            address = address_element.text.strip()

            return price, name, url, address
            
        except Exception as e:
            logging.debug(f"HTML parsing error: {e}")
            return self._extract_from_data_attributes(nhs_card)
    
    def _extract_from_data_attributes(self, nhs_card):
        """
        Input: BeautifulSoup card element
        Output: Tuple of (price, name, url, address)
        Description: Fallback extraction using data attributes when HTML structure fails
        """
        try:
            name = nhs_card.get('data-community-name', '')
            price_low = nhs_card.get('data-price-low', '')
            price_high = nhs_card.get('data-price-high', '')
            city = nhs_card.get('data-city', '')
            state = nhs_card.get('data-state-abbreviation', '')
            
            # Construct price range
            if price_low and price_high:
                price = f"${int(price_low):,} - ${int(price_high):,}"
            elif price_low:
                price = f"${int(price_low):,}"
            else:
                price = "Price not available"
            
            # Construct address
            address = f"{city}, {state}" if city and state else "Address not available"
            
            # Find URL in anchor tag
            url_element = nhs_card.find('a')
            url = url_element.get('href') if url_element else None
            
            if name and url:
                logging.debug(f"Successfully extracted from data attributes: {name}")
                return price, name, url, address
            else:
                raise ValueError("Insufficient data in attributes")
                
        except Exception as e:
            logging.debug(f"Data attribute extraction failed: {e}")
            raise ValueError("All parsing methods failed")

    def _create_document_from_json_ld(self, data: Dict, source_url: str, location_info: Dict = None) -> Optional[Dict]:
        """
        Input: JSON-LD data dict, source URL
        Output: Standardized document dict or None
        Description: Create database document from JSON-LD structured data
        """
        listing_id = self._generate_listing_id_from_json_ld(data)
        if not listing_id:
            return None
        
        # Add county information to address if location_info available
        if location_info and location_info.get("display_name"):
            data = self._add_county_to_address(data, location_info)
            
        return {
            "listing_id": listing_id,
            "scraped_at": datetime.now(),
            "source_url": source_url,
            "property_data": data,
            "data_source": "json_ld",
            "listing_status": "pending"  # Will be updated during database processing
        }

    def _create_document_from_html(self, data: Dict, source_url: str, location_info: Dict = None) -> Optional[Dict]:
        """
        Input: HTML-extracted data dict, source URL
        Output: Standardized document dict or None
        Description: Create database document from HTML-extracted data
        """
        listing_id = self._generate_listing_id_from_html(data)
        if not listing_id:
            return None
        
        # Add county information for HTML data (create minimal address structure)
        if location_info and location_info.get("display_name"):
            county = self._extract_county_from_display_name(location_info["display_name"])
            if county:
                # Add county to existing address or create new address structure
                if "address" not in data:
                    data["address"] = {}
                if isinstance(data["address"], str):
                    # Convert string address to object with county
                    original_address = data["address"]
                    data["address"] = {"formatted_address": original_address, "county": county}
                elif isinstance(data["address"], dict):
                    data["address"]["county"] = county
            
        return {
            "listing_id": listing_id,
            "scraped_at": datetime.now(), 
            "source_url": source_url,
            "property_data": data,
            "data_source": "html_fallback",
            "listing_status": "pending"  # Will be updated during database processing
        }

    def _generate_listing_id_from_json_ld(self, data: Dict) -> Optional[str]:
        """
        Input: JSON-LD data dict
        Output: Unique listing ID string or None
        Description: Generate listing ID from URL and name for JSON-LD data
        """
        url = data.get("url")
        name = data.get("name")

        if not url or not name:
            logging.warning(f"🚨 Missing required fields for JSON-LD listing ID: URL={url}, Name={name}")
            return None

        name = name.replace(" ", "_")
        return f"{url}_{name}"

    def _generate_listing_id_from_html(self, data: Dict) -> Optional[str]:
        """
        Input: HTML data dict
        Output: Unique listing ID string or None
        Description: Generate listing ID from URL and name for HTML data
        """
        url = data.get("url")
        name = data.get("name")
        
        if not url or not name:
            logging.warning(f"🚨 Missing required fields for HTML listing ID: URL={url}, Name={name}")
            return None
        
        name = name.replace(" ", "_")
        return f"{url}_{name}"

    def _log_found_urls(self, extracted_data: List[Dict]):
        """
        Input: List of extracted listing documents
        Output: None (logging only)
        Description: Log discovered listing URLs for debugging
        """
        found_urls = []
        for item in extracted_data:
            listing_id = item.get("listing_id")
            if listing_id and "_" in listing_id:
                potential_url = listing_id.split("_")[0]
                if potential_url.startswith('http'):
                    found_urls.append(potential_url)
        
        if found_urls:
            logging.info(f"🔗 FOUND LISTING URLs:")
            for found_url in found_urls[:5]:  # Show first 5
                logging.info(f"   🏠 {found_url}")
            if len(found_urls) > 5:
                logging.info(f"   ... and {len(found_urls) - 5} more listings")

    def _extract_county_from_display_name(self, display_name: str) -> Optional[str]:
        """
        Input: Display name string (e.g., "Ventura County, CA")
        Output: County name string or None
        Description: Extract county name from display_name format
        """
        if not display_name:
            return None
        
        # Handle formats like "Ventura County, CA" or "Riverside County, CA"
        if "County" in display_name:
            parts = display_name.split(",")
            if parts:
                county_part = parts[0].strip()
                if county_part.endswith(" County"):
                    return county_part
        
        return None

    def _add_county_to_address(self, data: Dict, location_info: Dict) -> Dict:
        """
        Input: JSON-LD data dict, location info dict
        Output: Modified data dict with county added to address
        Description: Add county information to JSON-LD address structure
        """
        county = self._extract_county_from_display_name(location_info.get("display_name", ""))
        if not county:
            return data
        
        # Create a copy to avoid modifying original data
        data_copy = data.copy()
        
        # Handle address structure - JSON-LD format uses PostalAddress
        # Handle both "Address" (capital) and "address" (lowercase) field names
        address_field = None
        if "Address" in data_copy and isinstance(data_copy["Address"], dict):
            address_field = "Address"
        elif "address" in data_copy and isinstance(data_copy["address"], dict):
            address_field = "address"
        
        if address_field:
            # Add county to existing address object
            data_copy[address_field]["county"] = county
        elif "address" not in data_copy and "Address" not in data_copy:
            # Create minimal address structure with county
            data_copy["address"] = {
                "@type": "PostalAddress",
                "county": county
            }
        
        return data_copy

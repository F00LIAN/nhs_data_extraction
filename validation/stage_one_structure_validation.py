"""
Confirm that every listing that is scraped has the following structure before inserting into the database.

JSON-LD: Schema.org structured data with required fields for property listings
HTML fallback: Extracted data from HTML elements when JSON-LD is not available
"""
from typing import Dict, Any, Optional
import logging


def validate_json_ld_structure(json_ld_data: dict) -> bool:
    """
    Validate JSON-LD structured data format.
    
    Required fields:
    - @context: "https://schema.org"
    - @type: Must include at least one property type
    - name: Property/listing name
    - url: Direct URL to the listing
    - address: PostalAddress object with locality and region
    - offers: Offer object with price information
    """
    if not isinstance(json_ld_data, dict):
        logging.warning("JSON-LD data is not a dictionary")
        return False
    
    # Check @context
    if json_ld_data.get("@context") != "https://schema.org":
        logging.warning("Missing or invalid @context in JSON-LD")
        return False
    
    # Check @type - should be a list or string with property types
    type_field = json_ld_data.get("@type")
    if not type_field:
        logging.warning("Missing @type in JSON-LD")
        return False
    
    # Check required basic fields
    required_fields = ["name", "url"]
    for field in required_fields:
        if not json_ld_data.get(field):
            logging.warning(f"Missing required field '{field}' in JSON-LD")
            return False
    
    # Validate address structure
    address = json_ld_data.get("address")
    if not isinstance(address, dict):
        logging.warning("Missing or invalid address structure in JSON-LD")
        return False
    
    address_required = ["addressLocality", "addressRegion"]
    for field in address_required:
        if not address.get(field):
            logging.warning(f"Missing required address field '{field}' in JSON-LD")
            return False
    
    # Validate offers structure
    offers = json_ld_data.get("offers")
    if not isinstance(offers, dict):
        logging.warning("Missing or invalid offers structure in JSON-LD")
        return False
    
    if not offers.get("price"):
        logging.warning("Missing price in offers for JSON-LD")
        return False
    
    return True


def validate_html_fallback_structure(html_data: dict) -> bool:
    """
    Validate HTML fallback extracted data format.
    
    Required fields:
    - name: Property/listing name
    - url: Direct URL to the listing
    - price: Price information (can be range)
    - address: Address object with formatted_address
    """
    if not isinstance(html_data, dict):
        logging.warning("HTML fallback data is not a dictionary")
        return False
    
    # Check required basic fields
    required_fields = ["name", "url", "price"]
    for field in required_fields:
        if not html_data.get(field):
            logging.warning(f"Missing required field '{field}' in HTML fallback")
            return False
    
    # Validate address structure
    address = html_data.get("address")
    if not isinstance(address, dict):
        logging.warning("Missing or invalid address structure in HTML fallback")
        return False
    
    if not address.get("formatted_address"):
        logging.warning("Missing formatted_address in HTML fallback")
        return False
    
    return True


def validate_document_structure(document: Dict[str, Any]) -> bool:
    """
    Validate complete document structure before database insertion.
    
    Required document fields:
    - listing_id: Unique identifier URL
    - scraped_at: Timestamp
    - source_url: Source page URL
    - property_data: The actual property data (JSON-LD or HTML)
    - data_source: Either "json_ld" or "html_fallback"
    - listing_status: Status of the listing
    """
    if not isinstance(document, dict):
        logging.warning("Document is not a dictionary")
        return False
    
    # Check required document fields
    required_doc_fields = ["listing_id", "scraped_at", "source_url", "property_data", "data_source", "listing_status"]
    for field in required_doc_fields:
        if field not in document:
            logging.warning(f"Missing required document field '{field}'")
            return False
    
    # Validate data_source value
    data_source = document.get("data_source")
    if data_source not in ["json_ld", "html_fallback"]:
        logging.warning(f"Invalid data_source value: {data_source}")
        return False
    
    # Validate property_data based on data_source
    property_data = document.get("property_data")
    if data_source == "json_ld":
        if not validate_json_ld_structure(property_data):
            logging.warning("Invalid JSON-LD property data structure")
            return False
    elif data_source == "html_fallback":
        if not validate_html_fallback_structure(property_data):
            logging.warning("Invalid HTML fallback property data structure")
            return False
    
    # Validate listing_id format (should be a URL)
    listing_id = document.get("listing_id")
    if not isinstance(listing_id, str) or not listing_id.startswith("http"):
        logging.warning("listing_id should be a valid URL")
        return False
    
    return True








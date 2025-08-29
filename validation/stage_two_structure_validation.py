"""
Stage Two Structure Validation for Community Data Collection

Validates community data structures before inserting into the communitydata collection.
Supports both base structures (initial scrapes) and change structures (with change summaries).
"""
from typing import Dict, Any, List
import logging


def validate_base_community_structure(community_data: Dict[str, Any]) -> bool:
    """
    Validate base community data structure for initial scrapes.
    
    Required fields for base structure:
    - listing_id: URL of the parent community
    - community_data: Object containing communities array
    - last_updated: Timestamp 
    - listing_status: Status of the listing
    - scraped_at: Timestamp when scraped
    - total_communities_found: Number of communities found
    """
    if not isinstance(community_data, dict):
        logging.warning("Community data is not a dictionary")
        return False
    
    # Check required top-level fields
    required_fields = ["listing_id", "community_data", "last_updated", "listing_status", "scraped_at", "total_communities_found"]
    for field in required_fields:
        if field not in community_data:
            logging.warning(f"Missing required field '{field}' in community data")
            return False
    
    # Validate listing_id format (should be a URL)
    listing_id = community_data.get("listing_id")
    if not isinstance(listing_id, str) or not listing_id.startswith("http"):
        logging.warning("listing_id should be a valid URL")
        return False
    
    # Validate community_data structure
    community_data_obj = community_data.get("community_data")
    if not isinstance(community_data_obj, dict):
        logging.warning("community_data should be an object")
        return False
    
    communities = community_data_obj.get("communities")
    if not isinstance(communities, list):
        logging.warning("communities should be an array")
        return False
    
    # Validate each community in the array
    for idx, community in enumerate(communities):
        if not validate_community_item_structure(community, idx):
            return False
    
    # Validate total_communities_found matches actual count
    total_found = community_data.get("total_communities_found")
    if not isinstance(total_found, int) or total_found != len(communities):
        logging.warning(f"total_communities_found ({total_found}) doesn't match communities array length ({len(communities)})")
        return False
    
    return True


def validate_change_community_structure(community_data: Dict[str, Any]) -> bool:
    """
    Validate community data structure with change tracking.
    
    Required fields for change structure (includes all base fields plus):
    - change_summary: Object with change statistics
    - previous_scraped_at: Previous scrape timestamp
    """
    # First validate base structure
    if not validate_base_community_structure(community_data):
        return False
    
    # Check change-specific fields
    change_fields = ["change_summary", "previous_scraped_at"]
    for field in change_fields:
        if field not in community_data:
            logging.warning(f"Missing required change field '{field}' in community data")
            return False
    
    # Validate change_summary structure
    change_summary = community_data.get("change_summary")
    if not isinstance(change_summary, dict):
        logging.warning("change_summary should be an object")
        return False
    
    required_change_fields = ["new_communities", "updated_communities", "removed_communities", "total_changes"]
    for field in required_change_fields:
        if field not in change_summary:
            logging.warning(f"Missing required field '{field}' in change_summary")
            return False
        if not isinstance(change_summary[field], int):
            logging.warning(f"Field '{field}' in change_summary should be an integer")
            return False
    
    return True


def validate_community_item_structure(community: Dict[str, Any], index: int = None) -> bool:
    """
    Validate individual community item structure.
    
    Required fields for each community:
    - build_status: Array of build statuses
    - name: Community name
    - url: Community URL
    - image: Image URL
    - build_type: Type (spec/plan)
    - price: Price value
    - price_currency: Currency code
    - community_id: Unique identifier URL
    - card_index: Index position
    - county: County name
    - address_locality: City/locality
    - postal_code: ZIP code
    """
    if not isinstance(community, dict):
        logging.warning(f"Community item {index} is not a dictionary")
        return False
    
    # Check required fields
    required_fields = [
        "build_status", "name", "url", "image", "build_type", 
        "price", "price_currency", "community_id", "card_index",
        "county", "address_locality", "postal_code"
    ]
    
    for field in required_fields:
        if field not in community:
            logging.warning(f"Missing required field '{field}' in community item {index}")
            return False
    
    # Validate build_status is an array
    build_status = community.get("build_status")
    if not isinstance(build_status, list) or len(build_status) == 0:
        logging.warning(f"build_status should be a non-empty array in community item {index}")
        return False
    
    # Validate URLs
    url_fields = ["url", "image", "community_id"]
    for field in url_fields:
        value = community.get(field)
        if not isinstance(value, str) or not value.startswith("http"):
            logging.warning(f"Field '{field}' should be a valid URL in community item {index}")
            return False
    
    # Validate build_type
    build_type = community.get("build_type")
    if build_type not in ["spec", "plan"]:
        logging.warning(f"build_type should be 'spec' or 'plan' in community item {index}")
        return False
    
    # Validate price and currency
    price = community.get("price")
    if not isinstance(price, str) or not price.isdigit():
        logging.warning(f"price should be a numeric string in community item {index}")
        return False
    
    price_currency = community.get("price_currency")
    if price_currency != "USD":
        logging.warning(f"price_currency should be 'USD' in community item {index}")
        return False
    
    # Validate card_index
    card_index = community.get("card_index")
    if not isinstance(card_index, int) or card_index < 0:
        logging.warning(f"card_index should be a non-negative integer in community item {index}")
        return False
    
    return True


def validate_community_document_structure(document: Dict[str, Any]) -> bool:
    """
    Main validation function for community documents.
    
    Automatically detects whether document is base or change structure
    and applies appropriate validation.
    """
    if not isinstance(document, dict):
        logging.warning("Document is not a dictionary")
        return False
    
    # Determine if this is a change document or base document
    has_change_fields = "change_summary" in document and "previous_scraped_at" in document
    
    if has_change_fields:
        return validate_change_community_structure(document)
    else:
        return validate_base_community_structure(document)








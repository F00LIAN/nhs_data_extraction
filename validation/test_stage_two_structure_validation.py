"""
Test suite for stage two structure validation functions.

Tests validation for community data structures (base and change)
before database insertion in the communitydata collection.
"""

import unittest
from datetime import datetime
from stage_two_structure_validation import (
    validate_base_community_structure,
    validate_change_community_structure,
    validate_community_item_structure,
    validate_community_document_structure
)


class TestStageTwoStructureValidation(unittest.TestCase):
    
    def setUp(self):
        """Set up test data based on actual architecture examples."""
        
        # Valid community item (based on actual architecture)
        self.valid_community_item = {
            "build_status": ["Hot Deal", "Under Construction"],
            "name": "Lenora",
            "url": "https://www.newhomesource.com/specdetail/1865-coffeeberry-road-santa-paula-ca-93060/2987214",
            "image": "https://nhs-dynamic-secure.akamaized.net/Images/Homes/Richmond/84609634-241010.jpg",
            "build_type": "spec",
            "price": "781693",
            "price_currency": "USD",
            "community_id": "https://www.newhomesource.com/specdetail/1865-coffeeberry-road-santa-paula-ca-93060/2987214_Lenora",
            "card_index": 0,
            "county": "Ventura County",
            "address_locality": "Santa Paula",
            "postal_code": "93060"
        }
        
        # Valid base structure (initial scrape)
        self.valid_base_document = {
            "listing_id": "https://www.newhomesource.com/community/ca/santa-paula/autumnwood-at-harvest-at-limoneira-by-richmond-american-homes/200287",
            "community_data": {
                "communities": [self.valid_community_item]
            },
            "last_updated": datetime.now(),
            "listing_status": "active",
            "scraped_at": datetime.now().isoformat(),
            "total_communities_found": 1
        }
        
        # Valid change structure (with change tracking)
        self.valid_change_document = {
            "listing_id": "https://www.newhomesource.com/community/ca/ventura/fresco-at-del-sol-by-shea-homes/176723",
            "community_data": {
                "communities": [self.valid_community_item]
            },
            "last_updated": datetime.now(),
            "listing_status": "active",
            "scraped_at": datetime.now().isoformat(),
            "total_communities_found": 1,
            "change_summary": {
                "new_communities": 1,
                "updated_communities": 0,
                "removed_communities": 0,
                "total_changes": 1
            },
            "previous_scraped_at": datetime.now().isoformat()
        }

    def test_valid_community_item_structure(self):
        """Test validation of valid community item."""
        result = validate_community_item_structure(self.valid_community_item)
        self.assertTrue(result)

    def test_community_item_missing_build_status(self):
        """Test community item validation fails without build_status."""
        invalid_item = self.valid_community_item.copy()
        del invalid_item["build_status"]
        result = validate_community_item_structure(invalid_item)
        self.assertFalse(result)

    def test_community_item_empty_build_status(self):
        """Test community item validation fails with empty build_status."""
        invalid_item = self.valid_community_item.copy()
        invalid_item["build_status"] = []
        result = validate_community_item_structure(invalid_item)
        self.assertFalse(result)

    def test_community_item_invalid_build_status_type(self):
        """Test community item validation fails with non-array build_status."""
        invalid_item = self.valid_community_item.copy()
        invalid_item["build_status"] = "not an array"
        result = validate_community_item_structure(invalid_item)
        self.assertFalse(result)

    def test_community_item_missing_name(self):
        """Test community item validation fails without name."""
        invalid_item = self.valid_community_item.copy()
        del invalid_item["name"]
        result = validate_community_item_structure(invalid_item)
        self.assertFalse(result)

    def test_community_item_invalid_url(self):
        """Test community item validation fails with invalid URL."""
        invalid_item = self.valid_community_item.copy()
        invalid_item["url"] = "not_a_url"
        result = validate_community_item_structure(invalid_item)
        self.assertFalse(result)

    def test_community_item_invalid_image(self):
        """Test community item validation fails with invalid image URL."""
        invalid_item = self.valid_community_item.copy()
        invalid_item["image"] = "not_a_url"
        result = validate_community_item_structure(invalid_item)
        self.assertFalse(result)

    def test_community_item_invalid_build_type(self):
        """Test community item validation fails with invalid build_type."""
        invalid_item = self.valid_community_item.copy()
        invalid_item["build_type"] = "invalid_type"
        result = validate_community_item_structure(invalid_item)
        self.assertFalse(result)

    def test_community_item_valid_plan_build_type(self):
        """Test community item validation passes with 'plan' build_type."""
        valid_item = self.valid_community_item.copy()
        valid_item["build_type"] = "plan"
        result = validate_community_item_structure(valid_item)
        self.assertTrue(result)

    def test_community_item_invalid_price_format(self):
        """Test community item validation fails with non-numeric price."""
        invalid_item = self.valid_community_item.copy()
        invalid_item["price"] = "not_numeric"
        result = validate_community_item_structure(invalid_item)
        self.assertFalse(result)

    def test_community_item_invalid_price_currency(self):
        """Test community item validation fails with non-USD currency."""
        invalid_item = self.valid_community_item.copy()
        invalid_item["price_currency"] = "EUR"
        result = validate_community_item_structure(invalid_item)
        self.assertFalse(result)

    def test_community_item_invalid_card_index(self):
        """Test community item validation fails with negative card_index."""
        invalid_item = self.valid_community_item.copy()
        invalid_item["card_index"] = -1
        result = validate_community_item_structure(invalid_item)
        self.assertFalse(result)

    def test_community_item_not_dict(self):
        """Test community item validation fails if item is not a dictionary."""
        result = validate_community_item_structure("not a dict")
        self.assertFalse(result)

    def test_valid_base_community_structure(self):
        """Test validation of valid base community structure."""
        result = validate_base_community_structure(self.valid_base_document)
        self.assertTrue(result)

    def test_base_community_missing_listing_id(self):
        """Test base community validation fails without listing_id."""
        invalid_doc = self.valid_base_document.copy()
        del invalid_doc["listing_id"]
        result = validate_base_community_structure(invalid_doc)
        self.assertFalse(result)

    def test_base_community_invalid_listing_id(self):
        """Test base community validation fails with invalid listing_id."""
        invalid_doc = self.valid_base_document.copy()
        invalid_doc["listing_id"] = "not_a_url"
        result = validate_base_community_structure(invalid_doc)
        self.assertFalse(result)

    def test_base_community_missing_community_data(self):
        """Test base community validation fails without community_data."""
        invalid_doc = self.valid_base_document.copy()
        del invalid_doc["community_data"]
        result = validate_base_community_structure(invalid_doc)
        self.assertFalse(result)

    def test_base_community_invalid_community_data_type(self):
        """Test base community validation fails with invalid community_data type."""
        invalid_doc = self.valid_base_document.copy()
        invalid_doc["community_data"] = "not an object"
        result = validate_base_community_structure(invalid_doc)
        self.assertFalse(result)

    def test_base_community_missing_communities_array(self):
        """Test base community validation fails without communities array."""
        invalid_doc = self.valid_base_document.copy()
        invalid_doc["community_data"] = {}
        result = validate_base_community_structure(invalid_doc)
        self.assertFalse(result)

    def test_base_community_invalid_communities_type(self):
        """Test base community validation fails with invalid communities type."""
        invalid_doc = self.valid_base_document.copy()
        invalid_doc["community_data"]["communities"] = "not an array"
        result = validate_base_community_structure(invalid_doc)
        self.assertFalse(result)

    def test_base_community_total_mismatch(self):
        """Test base community validation fails when total_communities_found doesn't match array length."""
        invalid_doc = self.valid_base_document.copy()
        invalid_doc["total_communities_found"] = 5  # But only 1 community in array
        result = validate_base_community_structure(invalid_doc)
        self.assertFalse(result)

    def test_base_community_invalid_community_item(self):
        """Test base community validation fails with invalid community item."""
        invalid_doc = self.valid_base_document.copy()
        invalid_doc["community_data"]["communities"] = [{"invalid": "item"}]
        result = validate_base_community_structure(invalid_doc)
        self.assertFalse(result)

    def test_base_community_not_dict(self):
        """Test base community validation fails if document is not a dictionary."""
        result = validate_base_community_structure("not a dict")
        self.assertFalse(result)

    def test_valid_change_community_structure(self):
        """Test validation of valid change community structure."""
        result = validate_change_community_structure(self.valid_change_document)
        self.assertTrue(result)

    def test_change_community_missing_change_summary(self):
        """Test change community validation fails without change_summary."""
        invalid_doc = self.valid_change_document.copy()
        del invalid_doc["change_summary"]
        result = validate_change_community_structure(invalid_doc)
        self.assertFalse(result)

    def test_change_community_missing_previous_scraped_at(self):
        """Test change community validation fails without previous_scraped_at."""
        invalid_doc = self.valid_change_document.copy()
        del invalid_doc["previous_scraped_at"]
        result = validate_change_community_structure(invalid_doc)
        self.assertFalse(result)

    def test_change_community_invalid_change_summary_type(self):
        """Test change community validation fails with invalid change_summary type."""
        invalid_doc = self.valid_change_document.copy()
        invalid_doc["change_summary"] = "not an object"
        result = validate_change_community_structure(invalid_doc)
        self.assertFalse(result)

    def test_change_community_missing_change_fields(self):
        """Test change community validation fails with missing change summary fields."""
        invalid_doc = self.valid_change_document.copy()
        del invalid_doc["change_summary"]["new_communities"]
        result = validate_change_community_structure(invalid_doc)
        self.assertFalse(result)

    def test_change_community_invalid_change_field_type(self):
        """Test change community validation fails with non-integer change fields."""
        invalid_doc = self.valid_change_document.copy()
        invalid_doc["change_summary"]["new_communities"] = "not an integer"
        result = validate_change_community_structure(invalid_doc)
        self.assertFalse(result)

    def test_change_community_invalid_base_structure(self):
        """Test change community validation fails if base structure is invalid."""
        invalid_doc = self.valid_change_document.copy()
        del invalid_doc["listing_id"]  # Make base structure invalid
        result = validate_change_community_structure(invalid_doc)
        self.assertFalse(result)

    def test_validate_community_document_structure_base(self):
        """Test main validation function correctly identifies and validates base structure."""
        result = validate_community_document_structure(self.valid_base_document)
        self.assertTrue(result)

    def test_validate_community_document_structure_change(self):
        """Test main validation function correctly identifies and validates change structure."""
        result = validate_community_document_structure(self.valid_change_document)
        self.assertTrue(result)

    def test_validate_community_document_structure_invalid_base(self):
        """Test main validation function correctly rejects invalid base structure."""
        invalid_doc = self.valid_base_document.copy()
        del invalid_doc["listing_id"]
        result = validate_community_document_structure(invalid_doc)
        self.assertFalse(result)

    def test_validate_community_document_structure_invalid_change(self):
        """Test main validation function correctly rejects invalid change structure."""
        invalid_doc = self.valid_change_document.copy()
        del invalid_doc["change_summary"]
        del invalid_doc["listing_id"]  # Make it invalid for base validation too
        result = validate_community_document_structure(invalid_doc)
        self.assertFalse(result)

    def test_validate_community_document_structure_not_dict(self):
        """Test main validation function fails if document is not a dictionary."""
        result = validate_community_document_structure("not a dict")
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main()

"""
Test suite for stage one structure validation functions.

Tests validation for both JSON-LD and HTML fallback data structures
before database insertion in the homepagedata collection.
"""

import unittest
from datetime import datetime
from stage_one_structure_validation import (
    validate_json_ld_structure,
    validate_html_fallback_structure,
    validate_document_structure
)


class TestStageOneStructureValidation(unittest.TestCase):
    
    def setUp(self):
        """Set up test data based on actual architecture examples."""
        
        # Valid JSON-LD data (based on homepagedata_json_architecture.json)
        self.valid_json_ld = {
            "@context": "https://schema.org",
            "@type": ["Product", "SingleFamilyResidence", "House"],
            "name": "Sultana at Heirloom Farms subdivision by Meritage Homes",
            "url": "https://www.newhomesource.com/community/ca/temecula/sultana-at-heirloom-farms-by-meritage-homes/170692",
            "address": {
                "@type": "PostalAddress",
                "addressLocality": "Temecula",
                "addressRegion": "CA",
                "postalCode": "92591",
                "streetAddress": "29001 Bucle Prado",
                "county": "Riverside County"
            },
            "offers": {
                "@type": "Offer",
                "price": "624000",
                "priceCurrency": "USD"
            }
        }
        
        # Valid HTML fallback data (based on homepagedata_html_fallback_architecture.json)
        self.valid_html_fallback = {
            "name": "Bedford",
            "url": "https://www.newhomesource.com/masterplan/ca/corona/bedford/186347",
            "price": "$628,476 - $1,080,506",
            "address": {
                "formatted_address": "Corona, CA 92883",
                "county": "Riverside County"
            }
        }
        
        # Valid complete document
        self.valid_document_json_ld = {
            "listing_id": "https://www.newhomesource.com/community/ca/temecula/sultana-at-heirloom-farms-by-meritage-homes/170692",
            "scraped_at": datetime.now(),
            "source_url": "https://www.newhomesource.com/communities/ca/riverside-san-bernardino-area/riverside-county/page-3",
            "property_data": self.valid_json_ld,
            "data_source": "json_ld",
            "listing_status": "active"
        }
        
        self.valid_document_html = {
            "listing_id": "https://www.newhomesource.com/masterplan/ca/corona/bedford/186347",
            "scraped_at": datetime.now(),
            "source_url": "https://www.newhomesource.com/communities/ca/riverside-san-bernardino-area/riverside-county/page-5",
            "property_data": self.valid_html_fallback,
            "data_source": "html_fallback",
            "listing_status": "active"
        }

    def test_valid_json_ld_structure(self):
        """Test validation of valid JSON-LD structure."""
        result = validate_json_ld_structure(self.valid_json_ld)
        self.assertTrue(result)

    def test_json_ld_missing_context(self):
        """Test JSON-LD validation fails without @context."""
        invalid_data = self.valid_json_ld.copy()
        del invalid_data["@context"]
        result = validate_json_ld_structure(invalid_data)
        self.assertFalse(result)

    def test_json_ld_wrong_context(self):
        """Test JSON-LD validation fails with wrong @context."""
        invalid_data = self.valid_json_ld.copy()
        invalid_data["@context"] = "https://example.com"
        result = validate_json_ld_structure(invalid_data)
        self.assertFalse(result)

    def test_json_ld_missing_type(self):
        """Test JSON-LD validation fails without @type."""
        invalid_data = self.valid_json_ld.copy()
        del invalid_data["@type"]
        result = validate_json_ld_structure(invalid_data)
        self.assertFalse(result)

    def test_json_ld_missing_name(self):
        """Test JSON-LD validation fails without name."""
        invalid_data = self.valid_json_ld.copy()
        del invalid_data["name"]
        result = validate_json_ld_structure(invalid_data)
        self.assertFalse(result)

    def test_json_ld_missing_url(self):
        """Test JSON-LD validation fails without url."""
        invalid_data = self.valid_json_ld.copy()
        del invalid_data["url"]
        result = validate_json_ld_structure(invalid_data)
        self.assertFalse(result)

    def test_json_ld_invalid_address_structure(self):
        """Test JSON-LD validation fails with invalid address."""
        invalid_data = self.valid_json_ld.copy()
        invalid_data["address"] = "invalid address format"
        result = validate_json_ld_structure(invalid_data)
        self.assertFalse(result)

    def test_json_ld_missing_address_locality(self):
        """Test JSON-LD validation fails without addressLocality."""
        invalid_data = self.valid_json_ld.copy()
        del invalid_data["address"]["addressLocality"]
        result = validate_json_ld_structure(invalid_data)
        self.assertFalse(result)

    def test_json_ld_missing_address_region(self):
        """Test JSON-LD validation fails without addressRegion."""
        invalid_data = self.valid_json_ld.copy()
        del invalid_data["address"]["addressRegion"]
        result = validate_json_ld_structure(invalid_data)
        self.assertFalse(result)

    def test_json_ld_invalid_offers_structure(self):
        """Test JSON-LD validation fails with invalid offers."""
        invalid_data = self.valid_json_ld.copy()
        invalid_data["offers"] = "invalid offers format"
        result = validate_json_ld_structure(invalid_data)
        self.assertFalse(result)

    def test_json_ld_missing_price(self):
        """Test JSON-LD validation fails without price in offers."""
        invalid_data = self.valid_json_ld.copy()
        del invalid_data["offers"]["price"]
        result = validate_json_ld_structure(invalid_data)
        self.assertFalse(result)

    def test_json_ld_not_dict(self):
        """Test JSON-LD validation fails if data is not a dictionary."""
        result = validate_json_ld_structure("not a dict")
        self.assertFalse(result)

    def test_valid_html_fallback_structure(self):
        """Test validation of valid HTML fallback structure."""
        result = validate_html_fallback_structure(self.valid_html_fallback)
        self.assertTrue(result)

    def test_html_fallback_missing_name(self):
        """Test HTML fallback validation fails without name."""
        invalid_data = self.valid_html_fallback.copy()
        del invalid_data["name"]
        result = validate_html_fallback_structure(invalid_data)
        self.assertFalse(result)

    def test_html_fallback_missing_url(self):
        """Test HTML fallback validation fails without url."""
        invalid_data = self.valid_html_fallback.copy()
        del invalid_data["url"]
        result = validate_html_fallback_structure(invalid_data)
        self.assertFalse(result)

    def test_html_fallback_missing_price(self):
        """Test HTML fallback validation fails without price."""
        invalid_data = self.valid_html_fallback.copy()
        del invalid_data["price"]
        result = validate_html_fallback_structure(invalid_data)
        self.assertFalse(result)

    def test_html_fallback_invalid_address_structure(self):
        """Test HTML fallback validation fails with invalid address."""
        invalid_data = self.valid_html_fallback.copy()
        invalid_data["address"] = "invalid address format"
        result = validate_html_fallback_structure(invalid_data)
        self.assertFalse(result)

    def test_html_fallback_missing_formatted_address(self):
        """Test HTML fallback validation fails without formatted_address."""
        invalid_data = self.valid_html_fallback.copy()
        del invalid_data["address"]["formatted_address"]
        result = validate_html_fallback_structure(invalid_data)
        self.assertFalse(result)

    def test_html_fallback_not_dict(self):
        """Test HTML fallback validation fails if data is not a dictionary."""
        result = validate_html_fallback_structure("not a dict")
        self.assertFalse(result)

    def test_valid_document_json_ld(self):
        """Test validation of valid complete JSON-LD document."""
        result = validate_document_structure(self.valid_document_json_ld)
        self.assertTrue(result)

    def test_valid_document_html_fallback(self):
        """Test validation of valid complete HTML fallback document."""
        result = validate_document_structure(self.valid_document_html)
        self.assertTrue(result)

    def test_document_missing_listing_id(self):
        """Test document validation fails without listing_id."""
        invalid_doc = self.valid_document_json_ld.copy()
        del invalid_doc["listing_id"]
        result = validate_document_structure(invalid_doc)
        self.assertFalse(result)

    def test_document_missing_scraped_at(self):
        """Test document validation fails without scraped_at."""
        invalid_doc = self.valid_document_json_ld.copy()
        del invalid_doc["scraped_at"]
        result = validate_document_structure(invalid_doc)
        self.assertFalse(result)

    def test_document_missing_source_url(self):
        """Test document validation fails without source_url."""
        invalid_doc = self.valid_document_json_ld.copy()
        del invalid_doc["source_url"]
        result = validate_document_structure(invalid_doc)
        self.assertFalse(result)

    def test_document_missing_property_data(self):
        """Test document validation fails without property_data."""
        invalid_doc = self.valid_document_json_ld.copy()
        del invalid_doc["property_data"]
        result = validate_document_structure(invalid_doc)
        self.assertFalse(result)

    def test_document_missing_data_source(self):
        """Test document validation fails without data_source."""
        invalid_doc = self.valid_document_json_ld.copy()
        del invalid_doc["data_source"]
        result = validate_document_structure(invalid_doc)
        self.assertFalse(result)

    def test_document_missing_listing_status(self):
        """Test document validation fails without listing_status."""
        invalid_doc = self.valid_document_json_ld.copy()
        del invalid_doc["listing_status"]
        result = validate_document_structure(invalid_doc)
        self.assertFalse(result)

    def test_document_invalid_data_source(self):
        """Test document validation fails with invalid data_source."""
        invalid_doc = self.valid_document_json_ld.copy()
        invalid_doc["data_source"] = "invalid_source"
        result = validate_document_structure(invalid_doc)
        self.assertFalse(result)

    def test_document_invalid_listing_id_format(self):
        """Test document validation fails with invalid listing_id format."""
        invalid_doc = self.valid_document_json_ld.copy()
        invalid_doc["listing_id"] = "not_a_url"
        result = validate_document_structure(invalid_doc)
        self.assertFalse(result)

    def test_document_invalid_property_data_for_json_ld(self):
        """Test document validation fails with invalid property_data for JSON-LD source."""
        invalid_doc = self.valid_document_json_ld.copy()
        invalid_doc["property_data"] = {"invalid": "data"}
        result = validate_document_structure(invalid_doc)
        self.assertFalse(result)

    def test_document_invalid_property_data_for_html_fallback(self):
        """Test document validation fails with invalid property_data for HTML fallback source."""
        invalid_doc = self.valid_document_html.copy()
        invalid_doc["property_data"] = {"invalid": "data"}
        result = validate_document_structure(invalid_doc)
        self.assertFalse(result)

    def test_document_not_dict(self):
        """Test document validation fails if document is not a dictionary."""
        result = validate_document_structure("not a dict")
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()

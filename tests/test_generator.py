"""
Tests for the VRS generator module.
"""

import unittest
import sys
import os

# Add the parent directory to sys.path to import aiva_vrs during testing
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aiva_vrs import (
    generate_vrs_id,
    normalize_chromosome,
    parse_vrs_id,
    is_valid_vrs_id,
    get_chromosome_from_vrs_id,
    get_sql_table_for_variant
)

class TestVrsGenerator(unittest.TestCase):
    """Test cases for the VRS generator module."""
    
    def test_normalize_chromosome(self):
        """Test chromosome normalization."""
        self.assertEqual(normalize_chromosome("chr1"), "1")
        self.assertEqual(normalize_chromosome("1"), "1")
        self.assertEqual(normalize_chromosome("chrX"), "X")
        self.assertEqual(normalize_chromosome("chrM"), "MT")
        self.assertEqual(normalize_chromosome("MT"), "MT")
        self.assertEqual(normalize_chromosome("chrUn"), "UN")
    
    def test_generate_vrs_id(self):
        """Test VRS ID generation."""
        # Test a deletion
        vrs_id = generate_vrs_id("chr7", 55174772, "GGAATTAAGAGAAGC", "", assembly="GRCh38")
        self.assertTrue(vrs_id.startswith("ga4gh:VA:7:"))
        
        # Test a SNP
        vrs_id = generate_vrs_id("chr17", 31350290, "C", "T", assembly="GRCh38")
        self.assertTrue(vrs_id.startswith("ga4gh:VA:17:"))
        
        # Test another SNP
        vrs_id = generate_vrs_id("chr11", 108244076, "C", "G", assembly="GRCh38")
        self.assertTrue(vrs_id.startswith("ga4gh:VA:11:"))
        
        # Test with non-chr prefix
        vrs_id1 = generate_vrs_id("chr7", 55174772, "G", "A", assembly="GRCh38")
        vrs_id2 = generate_vrs_id("7", 55174772, "G", "A", assembly="GRCh38")
        self.assertEqual(vrs_id1, vrs_id2)
        
        # Test special case
        vrs_id = generate_vrs_id("chrM", 1234, "A", "G", assembly="GRCh38")
        self.assertTrue(vrs_id.startswith("ga4gh:VA:MT:"))
    
    def test_is_valid_vrs_id(self):
        """Test VRS ID validation."""
        self.assertTrue(is_valid_vrs_id("ga4gh:VA:7:v9TQXvNOQeG1vNRVJCWlD_a1tRf_m2AP"))
        self.assertTrue(is_valid_vrs_id("ga4gh:VA:17:0WNx7PqRUIPudU4jNEi-rXwzzFfToSyM"))
        self.assertFalse(is_valid_vrs_id("invalid_id"))
        self.assertFalse(is_valid_vrs_id("ga4gh:XX:7:v9TQXvNOQeG1vNRVJCWlD_a1tRf_m2AP"))
        self.assertFalse(is_valid_vrs_id(""))
        self.assertFalse(is_valid_vrs_id(None))
    
    def test_parse_vrs_id(self):
        """Test VRS ID parsing."""
        components = parse_vrs_id("ga4gh:VA:7:v9TQXvNOQeG1vNRVJCWlD_a1tRf_m2AP")
        self.assertEqual(components["chromosome"], "7")
        self.assertEqual(components["digest"], "v9TQXvNOQeG1vNRVJCWlD_a1tRf_m2AP")
        self.assertEqual(components["type"], "VA")
        
        with self.assertRaises(ValueError):
            parse_vrs_id("invalid_id")
    
    def test_get_chromosome_from_vrs_id(self):
        """Test extracting chromosome from VRS ID."""
        chromosome = get_chromosome_from_vrs_id("ga4gh:VA:7:v9TQXvNOQeG1vNRVJCWlD_a1tRf_m2AP")
        self.assertEqual(chromosome, "7")
        
        chromosome = get_chromosome_from_vrs_id("ga4gh:VA:X:v9TQXvNOQeG1vNRVJCWlD_a1tRf_m2AP")
        self.assertEqual(chromosome, "X")
        
        with self.assertRaises(ValueError):
            get_chromosome_from_vrs_id("invalid_id")
    
    def test_get_sql_table_for_variant(self):
        """Test getting SQL table name for a variant."""
        table = get_sql_table_for_variant("ga4gh:VA:7:v9TQXvNOQeG1vNRVJCWlD_a1tRf_m2AP")
        self.assertEqual(table, "variants_chr7")
        
        table = get_sql_table_for_variant("ga4gh:VA:X:v9TQXvNOQeG1vNRVJCWlD_a1tRf_m2AP")
        self.assertEqual(table, "variants_chrx")
        
        with self.assertRaises(ValueError):
            get_sql_table_for_variant("invalid_id")

if __name__ == "__main__":
    unittest.main()

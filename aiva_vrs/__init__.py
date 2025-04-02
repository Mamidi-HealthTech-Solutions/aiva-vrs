"""
AIVA VRS Generator Package

This package provides tools for generating and parsing VRS (Variant Representation Specification) identifiers.
"""

from .generator import (
    generate_vrs_id,
    normalize_chromosome,
    parse_vrs_id,
    is_valid_vrs_id,
    get_chromosome_from_vrs_id,
    get_sql_table_for_variant,
    build_variant_query
)

__version__ = "0.1.0"

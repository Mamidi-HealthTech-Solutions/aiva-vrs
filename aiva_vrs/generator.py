"""
VRS Generator Module

This module provides functions for generating and parsing VRS identifiers for genomic variants.
"""

import hashlib
import base64
import re
from typing import Dict, Tuple, Optional, Union

# Regular expression for validating VRS IDs
VRS_ID_PATTERN = r'^ga4gh:VA:([^:]+):(.+)$'

def normalize_chromosome(chrom: str) -> str:
    """
    Normalize chromosome name for VRS ID representation
    
    Args:
        chrom (str): Chromosome name to normalize
        
    Returns:
        str: Normalized chromosome name
    """
    # First, handle 'chr' prefix - always remove it for VRS IDs
    if chrom.startswith('chr'):
        chrom = chrom[3:]
    
    # Handle special cases
    if chrom == 'M':
        return 'MT'
    elif chrom == 'MT':
        return 'MT'
    elif chrom == 'Un':
        return 'UN'
    
    # Return the normalized chromosome
    return chrom

def generate_vrs_id(chrom: str, pos: Union[str, int], ref: str, alt: str, assembly: str = 'GRCh38') -> str:
    """
    Generate a VRS identifier for a variant
    
    Args:
        chrom (str): Chromosome name
        pos (str or int): Variant position
        ref (str): Reference allele
        alt (str): Alternate allele
        assembly (str): Genome assembly (default: GRCh38)
        
    Returns:
        str: VRS identifier in ga4gh:VA format
    """
    try:
        # Normalize chromosome
        normalized_chrom = normalize_chromosome(chrom)
        
        # Handle special cases
        if alt == '*' or ref == '*':
            return f"ga4gh:VA:SPECIAL:{normalized_chrom}-{pos}-{ref}-{alt}"
        
        # Create a digest-based VRS ID
        # This is a simplified version of the GA4GH VRS digest algorithm
        data = f"{normalized_chrom}:{pos}:{ref}:{alt}".encode('utf-8')
        digest = hashlib.sha512(data).digest()
        trunc_digest = digest[:24]  # Use first 24 bytes
        b64_digest = base64.b64encode(trunc_digest).decode('utf-8')
        # Remove padding characters and replace URL-unsafe characters
        b64_digest = b64_digest.rstrip('=').replace('+', '-').replace('/', '_')
        
        # Format: ga4gh:VA:{chrom}:{digest}
        # Use the normalized chromosome as part of the identifier
        vrs_id = f"ga4gh:VA:{normalized_chrom}:{b64_digest}"
        
        return vrs_id
    except Exception as e:
        raise ValueError(f"Error generating VRS ID for {chrom}:{pos}:{ref}:{alt}: {e}")

def is_valid_vrs_id(vrs_id: str) -> bool:
    """
    Check if a string is a valid VRS identifier
    
    Args:
        vrs_id (str): The VRS identifier to validate
        
    Returns:
        bool: True if the identifier is valid, False otherwise
    """
    if not vrs_id:
        return False
    
    # Check if it matches the VRS ID pattern
    match = re.match(VRS_ID_PATTERN, vrs_id)
    if not match:
        return False
    
    # Additional validation could be added here
    return True

def parse_vrs_id(vrs_id: str) -> Dict[str, str]:
    """
    Parse a VRS identifier into its components
    
    Args:
        vrs_id (str): The VRS identifier to parse
        
    Returns:
        dict: A dictionary containing the components of the VRS ID
              - 'chromosome': The chromosome
              - 'digest': The digest part
              - 'type': The type of identifier (VA for variant)
              
    Raises:
        ValueError: If the VRS ID is invalid
    """
    if not is_valid_vrs_id(vrs_id):
        raise ValueError(f"Invalid VRS ID: {vrs_id}")
    
    # Extract components using regex
    match = re.match(VRS_ID_PATTERN, vrs_id)
    if not match:
        raise ValueError(f"Failed to parse VRS ID: {vrs_id}")
    
    chromosome = match.group(1)
    digest = match.group(2)
    
    return {
        'chromosome': chromosome,
        'digest': digest,
        'type': 'VA'  # Variant
    }

def get_chromosome_from_vrs_id(vrs_id: str) -> str:
    """
    Extract the chromosome from a VRS identifier
    
    Args:
        vrs_id (str): The VRS identifier
        
    Returns:
        str: The chromosome
        
    Raises:
        ValueError: If the VRS ID is invalid
    """
    components = parse_vrs_id(vrs_id)
    return components['chromosome']

def get_sql_table_for_variant(vrs_id: str) -> str:
    """
    Get the SQL table name for a variant based on its VRS ID
    
    Args:
        vrs_id (str): The VRS identifier
        
    Returns:
        str: The SQL table name
        
    Raises:
        ValueError: If the VRS ID is invalid
    """
    chromosome = get_chromosome_from_vrs_id(vrs_id)
    return f"variants_chr{chromosome.lower()}"

def build_variant_query(vrs_id: str) -> Tuple[str, Dict[str, str]]:
    """
    Build a SQL query to fetch a variant by its VRS ID
    
    Args:
        vrs_id (str): The VRS identifier
        
    Returns:
        tuple: (query_string, parameters)
        
    Raises:
        ValueError: If the VRS ID is invalid
    """
    table_name = get_sql_table_for_variant(vrs_id)
    chromosome = get_chromosome_from_vrs_id(vrs_id)
    
    query = f"""
    SELECT *
    FROM public.{table_name}
    WHERE id = %(vrs_id)s
    AND chromosome = %(chromosome)s
    """
    
    params = {
        'vrs_id': vrs_id,
        'chromosome': chromosome
    }
    
    return query, params

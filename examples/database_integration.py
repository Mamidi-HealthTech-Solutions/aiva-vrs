"""
Example of integrating the VRS ID Generator package with database operations.

This example demonstrates:
1. Generating VRS IDs for variants
2. Parsing VRS IDs to extract components
3. Building database queries based on VRS IDs
4. Using with different database libraries (psycopg2 and SQLAlchemy)
"""

import os
import sys
import csv
from typing import List, Dict, Any

# Add the parent directory to sys.path to import aiva_vrs during development
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aiva_vrs import (
    generate_vrs_id,
    parse_vrs_id,
    get_chromosome_from_vrs_id,
    get_sql_table_for_variant,
    build_variant_query
)

# Example 1: Generate VRS IDs for a list of variants
def generate_vrs_ids_for_variants(variants: List[Dict[str, Any]], assembly: str = 'GRCh38') -> List[Dict[str, Any]]:
    """
    Generate VRS IDs for a list of variants.
    
    Args:
        variants: List of dictionaries containing variant information
        assembly: Genome assembly
        
    Returns:
        List of dictionaries with VRS IDs added
    """
    for variant in variants:
        chrom = variant.get('chromosome', '')
        pos = variant.get('position', 0)
        ref = variant.get('reference_allele', '')
        alt = variant.get('alternate_allele', '')
        
        # Generate VRS ID
        vrs_id = generate_vrs_id(chrom, pos, ref, alt, assembly)
        variant['vrs_id'] = vrs_id
    
    return variants

# Example 2: Parse VRS IDs from a list of variant IDs
def parse_vrs_ids(vrs_ids: List[str]) -> List[Dict[str, str]]:
    """
    Parse a list of VRS IDs into their components.
    
    Args:
        vrs_ids: List of VRS identifiers
        
    Returns:
        List of dictionaries containing the components of each VRS ID
    """
    results = []
    
    for vrs_id in vrs_ids:
        try:
            components = parse_vrs_id(vrs_id)
            results.append({
                'vrs_id': vrs_id,
                'chromosome': components['chromosome'],
                'digest': components['digest'],
                'type': components['type']
            })
        except ValueError as e:
            results.append({
                'vrs_id': vrs_id,
                'error': str(e)
            })
    
    return results

# Example 3: Build SQL queries for a list of VRS IDs
def build_queries_for_vrs_ids(vrs_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Build SQL queries for a list of VRS IDs.
    
    Args:
        vrs_ids: List of VRS identifiers
        
    Returns:
        List of dictionaries containing SQL queries and parameters
    """
    results = []
    
    for vrs_id in vrs_ids:
        try:
            query, params = build_variant_query(vrs_id)
            table_name = get_sql_table_for_variant(vrs_id)
            
            results.append({
                'vrs_id': vrs_id,
                'table': table_name,
                'query': query,
                'params': params
            })
        except ValueError as e:
            results.append({
                'vrs_id': vrs_id,
                'error': str(e)
            })
    
    return results

# Example 4: Demonstrate with psycopg2 (commented out as it requires a database connection)
def example_with_psycopg2():
    """
    Example of using the VRS ID Generator package with psycopg2.
    This is commented out as it requires a database connection.
    """
    # Uncomment to run with an actual database
    """
    import psycopg2
    
    # Connect to the database
    conn = psycopg2.connect("dbname=aiva_db user=postgres")
    cursor = conn.cursor()
    
    # VRS ID to look up
    vrs_id = "ga4gh:VA:7:v9TQXvNOQeG1vNRVJCWlD_a1tRf_m2AP"
    
    # Build the query
    query, params = build_variant_query(vrs_id)
    
    # Execute the query
    cursor.execute(query, params)
    variant = cursor.fetchone()
    
    # Process the result
    if variant:
        print(f"Found variant: {variant}")
    else:
        print(f"Variant not found: {vrs_id}")
    
    # Close the connection
    cursor.close()
    conn.close()
    """
    pass

# Example 5: Demonstrate with SQLAlchemy (commented out as it requires a database connection)
def example_with_sqlalchemy():
    """
    Example of using the VRS ID Generator package with SQLAlchemy.
    This is commented out as it requires a database connection.
    """
    # Uncomment to run with an actual database
    """
    from sqlalchemy import create_engine, text
    
    # Create engine
    engine = create_engine("postgresql://postgres:password@localhost/aiva_db")
    
    # VRS ID to look up
    vrs_id = "ga4gh:VA:7:v9TQXvNOQeG1vNRVJCWlD_a1tRf_m2AP"
    
    # Get table name and chromosome
    table_name = get_sql_table_for_variant(vrs_id)
    chromosome = get_chromosome_from_vrs_id(vrs_id)
    
    # Build and execute query
    with engine.connect() as connection:
        query = text(f'''
            SELECT v.*, tc.* 
            FROM public.{table_name} v
            LEFT JOIN public.transcript_consequences tc ON v.id = tc.variant_id
            WHERE v.id = :vrs_id
            AND v.chromosome = :chromosome
        ''')
        
        result = connection.execute(query, {"vrs_id": vrs_id, "chromosome": chromosome})
        variants = result.fetchall()
        
        for variant in variants:
            print(f"Variant: {variant}")
    """
    pass

# Example 6: Process a CSV file and add VRS IDs
def process_csv_with_vrs_ids(input_file: str, output_file: str, assembly: str = 'GRCh38'):
    """
    Process a CSV file and add VRS IDs to each row.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
        assembly: Genome assembly
    """
    with open(input_file, 'r') as f_in, open(output_file, 'w', newline='') as f_out:
        reader = csv.DictReader(f_in)
        
        # Add VRS ID field to headers
        fieldnames = reader.fieldnames + ['vrs_id']
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        
        # Process each row
        for row in reader:
            # Extract variant information
            chrom = row.get('chromosome', '')
            pos = row.get('position', 0)
            ref = row.get('reference_allele', '')
            alt = row.get('alternate_allele', '')
            
            # Generate VRS ID
            vrs_id = generate_vrs_id(chrom, pos, ref, alt, assembly)
            row['vrs_id'] = vrs_id
            
            # Write to output
            writer.writerow(row)

if __name__ == "__main__":
    # Example usage
    print("Example 1: Generate VRS IDs for variants")
    variants = [
        {'chromosome': 'chr7', 'position': 55174772, 'reference_allele': 'GGAATTAAGAGAAGC', 'alternate_allele': ''},
        {'chromosome': 'chr17', 'position': 31350290, 'reference_allele': 'C', 'alternate_allele': 'T'},
        {'chromosome': 'chr11', 'position': 108244076, 'reference_allele': 'C', 'alternate_allele': 'G'}
    ]
    
    variants_with_ids = generate_vrs_ids_for_variants(variants)
    for variant in variants_with_ids:
        print(f"Variant: {variant['chromosome']}:{variant['position']} {variant['reference_allele']}>{variant['alternate_allele']}")
        print(f"VRS ID: {variant['vrs_id']}")
        print()
    
    print("\nExample 2: Parse VRS IDs")
    vrs_ids = [
        "ga4gh:VA:7:v9TQXvNOQeG1vNRVJCWlD_a1tRf_m2AP",
        "ga4gh:VA:17:0WNx7PqRUIPudU4jNEi-rXwzzFfToSyM",
        "ga4gh:VA:11:T2nN8TdyvZ398c9-JYRi0tY_QbQ4dw5s"
    ]
    
    parsed_ids = parse_vrs_ids(vrs_ids)
    for parsed in parsed_ids:
        print(f"VRS ID: {parsed['vrs_id']}")
        print(f"Chromosome: {parsed['chromosome']}")
        print(f"Digest: {parsed['digest']}")
        print()
    
    print("\nExample 3: Build SQL queries")
    queries = build_queries_for_vrs_ids(vrs_ids)
    for query_info in queries:
        print(f"VRS ID: {query_info['vrs_id']}")
        print(f"Table: {query_info['table']}")
        print(f"Query: {query_info['query'].strip()}")
        print(f"Params: {query_info['params']}")
        print()

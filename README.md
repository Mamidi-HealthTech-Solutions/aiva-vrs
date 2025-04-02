# VRS ID Generator

A Python package for generating and parsing VRS (Variant Representation Specification) identifiers for genomic variants.

This package was inspired by the [GA4GH Variant Representation Specification](https://vrs.ga4gh.org/en/stable/) (VRS) and provides a similar way to uniquely and precisely identify genomic variants. However, it is not a direct implementation of VRS and can have different IDs than the original VRS specification.

## Installation

```bash
# Install from PyPI (once published)
pip install aiva-vrs

# Install from local directory in development mode
pip install -e .
```

## Features

- Generate consistent VRS identifiers for genomic variants
- Parse VRS identifiers to extract components
- Validate VRS identifiers
- Build database queries for variant lookup
- Normalize chromosome representations
- Compliant with GA4GH VRS standard

## Why Use VRS IDs?

### The Problem with Traditional Variant Representation

Traditionally, genomic variants are represented using a combination of:
- Chromosome (e.g., "chr7" or "7")
- Position (e.g., 55174772)
- Reference allele (e.g., "G")
- Alternate allele (e.g., "A")

This approach has several significant challenges:

1. **Inconsistent Representation**: Different tools may represent the same variant differently:
   - Chromosome format inconsistencies (e.g., "chr7" vs "7")
   - Different normalization of complex variants
   - Representation of insertions/deletions varies between tools

2. **Database Querying Complexity**:
   - Querying by 4 separate fields is inefficient
   - Requires complex joins and indexing strategies
   - Difficult to maintain consistency across different data sources

3. **Cross-Reference Challenges**:
   - Matching variants between datasets is error-prone
   - No single identifier to track a variant across systems
   - Difficult to integrate data from multiple sources

### The VRS ID Solution

VRS IDs solve these problems by:

1. **Single Consistent Identifier**: Each variant gets a unique, stable identifier
2. **Deterministic Generation**: The same variant always gets the same ID
3. **Self-Contained Information**: The chromosome is encoded in the ID
4. **Efficient Database Operations**: Query by a single field instead of four
5. **Simplified Data Integration**: Easily match variants across different datasets

## Basic Usage

```python
from aiva_vrs import generate_vrs_id, parse_vrs_id, is_valid_vrs_id

# Generate a VRS ID
vrs_id = generate_vrs_id("chr7", 55174772, "GGAATTAAGAGAAGC", "", assembly="GRCh38")
print(vrs_id)  # ga4gh:VA:7:v9TQXvNOQeG1vNRVJCWlD_a1tRf_m2AP

# Validate a VRS ID
is_valid = is_valid_vrs_id(vrs_id)
print(is_valid)  # True

# Parse a VRS ID
components = parse_vrs_id(vrs_id)
print(components)  # {'chromosome': '7', 'digest': 'v9TQXvNOQeG1vNRVJCWlD_a1tRf_m2AP', 'type': 'VA'}
```

## Database Structure and Integration

### Why Chromosome-Based Tables?

Genomic variant databases often use a chromosome-based structure. This design provides several benefits:

1. **Performance**: Queries for variants on a specific chromosome are much faster
2. **Scalability**: Allows for parallel processing and sharding of data
3. **Maintenance**: Easier to manage and update data for specific chromosomes

### Database Schema

A typical genomic database might include these key tables:

```sql
-- One table per chromosome for variants
CREATE TABLE public.variants_chr1 (
    id TEXT PRIMARY KEY,           -- VRS ID (ga4gh:VA:...)
    chromosome TEXT NOT NULL,      -- Normalized chromosome (e.g., "1")
    position INTEGER NOT NULL,     -- Genomic position
    reference_allele TEXT NOT NULL, -- Reference allele
    alternate_allele TEXT NOT NULL, -- Alternate allele
    -- Additional fields...
);

-- Similar tables for other chromosomes (variants_chr2, variants_chr3, etc.)

-- Transcript consequences
CREATE TABLE public.transcript_consequences (
    id TEXT PRIMARY KEY,
    variant_id TEXT NOT NULL,      -- References a VRS ID
    transcript_id TEXT NOT NULL,
    -- Additional fields...
);

-- Sample variants (associations between samples and variants)
CREATE TABLE public.sample_variants (
    sample_id TEXT NOT NULL,
    variant_id TEXT NOT NULL,      -- References a VRS ID
    genotype TEXT,
    allele_frequency FLOAT,
    -- Additional fields...
    PRIMARY KEY (sample_id, variant_id)
);

-- Samples information
CREATE TABLE public.samples (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    variant_count INTEGER DEFAULT 0,
    -- Additional fields...
);
```

### How VRS IDs Connect the Database

The VRS ID serves as the primary identifier for variants across all tables:

1. Each variant has a unique VRS ID that includes the chromosome in its structure
2. The VRS generator creates consistent IDs for the same variant, even from different sources
3. The chromosome component of the VRS ID determines which table to query
4. Sample-variant associations use the VRS ID to link samples to their variants

This design enables efficient queries and ensures data consistency across the system.

## Database Integration Examples

### Example 1: Fetch a variant from the database

```python
import psycopg2
from aiva_vrs import build_variant_query

# Connect to the database
conn = psycopg2.connect("dbname=genomics_db user=postgres")
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
```

### Example 2: Using with SQLAlchemy

```python
from sqlalchemy import create_engine, text
from aiva_vrs import get_chromosome_from_vrs_id, get_sql_table_for_variant

# Create engine
engine = create_engine("postgresql://postgres:password@localhost/genomics_db")

# VRS ID to look up
vrs_id = "ga4gh:VA:7:v9TQXvNOQeG1vNRVJCWlD_a1tRf_m2AP"

# Get table name and chromosome
table_name = get_sql_table_for_variant(vrs_id)
chromosome = get_chromosome_from_vrs_id(vrs_id)

# Build and execute query
with engine.connect() as connection:
    query = text(f"""
        SELECT v.*, tc.* 
        FROM public.{table_name} v
        LEFT JOIN public.transcript_consequences tc ON v.id = tc.variant_id
        WHERE v.id = :vrs_id
        AND v.chromosome = :chromosome
    """)
    
    result = connection.execute(query, {"vrs_id": vrs_id, "chromosome": chromosome})
    variants = result.fetchall()
    
    for variant in variants:
        print(f"Variant: {variant}")
```

### Example 3: Cloud Function Integration

```python
import functions_framework
from google.cloud import bigquery
from aiva_vrs import parse_vrs_id

@functions_framework.http
def lookup_variant(request):
    """HTTP Cloud Function to look up a variant by VRS ID."""
    # Get VRS ID from request
    request_json = request.get_json(silent=True)
    vrs_id = request_json.get('vrs_id')
    
    if not vrs_id:
        return {'error': 'No VRS ID provided'}, 400
    
    try:
        # Parse the VRS ID
        components = parse_vrs_id(vrs_id)
        chromosome = components['chromosome']
        
        # Set up BigQuery client
        client = bigquery.Client()
        
        # Query for the variant
        query = f"""
            SELECT *
            FROM `project.dataset.variants_chr{chromosome.lower()}`
            WHERE id = @vrs_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("vrs_id", "STRING", vrs_id)
            ]
        )
        
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        
        # Format results
        variants = [dict(row) for row in results]
        
        if not variants:
            return {'message': f'No variant found for VRS ID: {vrs_id}'}, 404
        
        return {'variants': variants}, 200
        
    except ValueError as e:
        return {'error': str(e)}, 400
    except Exception as e:
        return {'error': f'Internal error: {str(e)}'}, 500
```

## Using in Processing variants from OpenCRAVAT CSV

```python
from aiva_vrs import generate_vrs_id
import csv
import gzip

def process_opencravat_csv(csv_path, output_dir, assembly='GRCh38', compress=True):
    """Process an OpenCRAVAT CSV file and generate CSVs for database import."""
    # Open the input file
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f, delimiter=',')
        
        # Prepare output files
        variants_file = f"{output_dir}/variants.csv.gz" if compress else f"{output_dir}/variants.csv"
        variants_out = gzip.open(variants_file, 'wt') if compress else open(variants_file, 'w')
        
        # Write headers
        variants_writer = csv.writer(variants_out)
        variants_writer.writerow(['id', 'chromosome', 'position', 'reference_allele', 'alternate_allele'])
        
        # Process each row
        for row in reader:
            # Extract variant information
            chrom = row.get('Chrom', '')
            pos = row.get('Pos', '')
            ref = row.get('Reference allele', '')
            alt = row.get('Alternate allele', '')
            
            # Generate VRS ID
            vrs_id = generate_vrs_id(chrom, pos, ref, alt, assembly)
            
            # Write variant data
            variants_writer.writerow([vrs_id, chrom, pos, ref, alt])
        
        # Close output files
        variants_out.close()
```

## Development

### Running Tests

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

## License

MIT License

"""
Example of using the VRS ID Generator package in a Google Cloud Function.

This demonstrates:
1. Processing variant data in a cloud environment
2. Generating VRS IDs for variants
3. Storing results in Cloud Storage
4. Using BigQuery for variant lookups

To deploy this function:
1. Install the Google Cloud SDK
2. Run: gcloud functions deploy process_variants --runtime python39 --trigger-http --allow-unauthenticated
"""

import functions_framework
import json
import csv
import io
import tempfile
from google.cloud import storage, bigquery
import os
import sys

# For local testing, add the parent directory to sys.path
# In production, the package would be installed via requirements.txt
try:
    from aiva_vrs import generate_vrs_id, parse_vrs_id, get_chromosome_from_vrs_id
except ImportError:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from aiva_vrs import generate_vrs_id, parse_vrs_id, get_chromosome_from_vrs_id

# Configuration
BATCH_SIZE = 1000  # Number of rows to process in memory before writing
DEFAULT_BUCKET = "genomics-variant-data"  # Default GCS bucket

@functions_framework.http
def process_variants(request):
    """
    Cloud Function to process variant data from a CSV file in Cloud Storage.
    
    Expected request format:
    {
        "input_file": "gs://bucket-name/path/to/input.csv",
        "output_bucket": "bucket-name",
        "output_prefix": "path/to/output/",
        "assembly": "GRCh38"
    }
    
    Returns:
        JSON response with processing results
    """
    # Parse request
    request_json = request.get_json(silent=True)
    
    if not request_json:
        return json.dumps({"error": "No JSON data provided"}), 400
    
    input_file = request_json.get("input_file")
    if not input_file:
        return json.dumps({"error": "No input_file specified"}), 400
    
    output_bucket = request_json.get("output_bucket", DEFAULT_BUCKET)
    output_prefix = request_json.get("output_prefix", "")
    assembly = request_json.get("assembly", "GRCh38")
    
    try:
        # Process the input file
        result = process_variant_file(input_file, output_bucket, output_prefix, assembly)
        return json.dumps(result), 200
    
    except Exception as e:
        return json.dumps({"error": str(e)}), 500

def process_variant_file(input_file, output_bucket, output_prefix, assembly):
    """
    Process a variant file from Cloud Storage and generate output files.
    
    Args:
        input_file: GCS path to input file (gs://bucket/path)
        output_bucket: GCS bucket for output
        output_prefix: Prefix for output files
        assembly: Genome assembly
        
    Returns:
        Dictionary with processing results
    """
    # Parse GCS path
    if input_file.startswith("gs://"):
        input_file = input_file[5:]
    
    bucket_name, blob_path = input_file.split("/", 1)
    
    # Initialize GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    # Get the filename without path
    file_name = os.path.basename(blob_path)
    base_name = os.path.splitext(file_name)[0]
    
    # Create in-memory buffers for output files
    variants_buffer = io.StringIO()
    sample_variants_buffer = io.StringIO()
    samples_buffer = io.StringIO()
    
    # Create CSV writers
    variants_writer = csv.writer(variants_buffer)
    sample_variants_writer = csv.writer(sample_variants_buffer)
    samples_writer = csv.writer(samples_buffer)
    
    # Write headers
    variants_writer.writerow(['id', 'chromosome', 'position', 'reference_allele', 'alternate_allele'])
    sample_variants_writer.writerow(['sample_id', 'variant_id', 'genotype', 'allele_depth', 'total_depth', 'allele_frequency', 'quality'])
    samples_writer.writerow(['id', 'name', 'description', 'owner_id', 'group_id', 'is_public', 'patient_id', 
                           'sample_type', 'collection_date', 'status', 'review_status', 'metadata', 
                           'clinical_notes', 'phenotype_terms', 'variant_count', 'created_at', 'updated_at', 
                           'last_accessed', 'access_count', 'view_status', 'tier_level'])
    
    # Download the blob to a temporary file
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp:
        blob.download_to_file(temp)
        temp_path = temp.name
    
    try:
        # Process the file in batches
        sample_variant_counts = {}
        batch_count = 0
        total_variants = 0
        
        with open(temp_path, 'r') as f:
            reader = csv.DictReader(f)
            
            # Initialize batch
            variants_batch = []
            sample_variants_batch = []
            
            # Process each row
            for row in reader:
                # Extract variant information
                chrom = row.get('Chrom', '')
                pos = row.get('Pos', '')
                ref = row.get('Reference allele', '')
                alt = row.get('Alternate allele', '')
                sample_id = row.get('Sample', '')
                
                # Skip if missing required fields
                if not all([chrom, pos, ref, alt, sample_id]):
                    continue
                
                # Generate VRS ID
                vrs_id = generate_vrs_id(chrom, pos, ref, alt, assembly)
                
                # Add to variants batch
                variants_batch.append([vrs_id, chrom, pos, ref, alt])
                
                # Extract sample variant information
                genotype = row.get('Genotype', '')
                allele_depth = row.get('Allele depth', '')
                total_depth = row.get('Total depth', '')
                allele_freq = row.get('Allele frequency', '')
                quality = row.get('Quality', '')
                
                # Add to sample variants batch
                sample_variants_batch.append([sample_id, vrs_id, genotype, allele_depth, total_depth, allele_freq, quality])
                
                # Update sample variant counts
                if sample_id not in sample_variant_counts:
                    sample_variant_counts[sample_id] = 0
                sample_variant_counts[sample_id] += 1
                
                # Write batch if it reaches the batch size
                if len(variants_batch) >= BATCH_SIZE:
                    # Write variants batch
                    for variant in variants_batch:
                        variants_writer.writerow(variant)
                    
                    # Write sample variants batch
                    for sample_variant in sample_variants_batch:
                        sample_variants_writer.writerow(sample_variant)
                    
                    # Clear batches
                    total_variants += len(variants_batch)
                    batch_count += 1
                    variants_batch = []
                    sample_variants_batch = []
            
            # Write any remaining items in the batch
            if variants_batch:
                for variant in variants_batch:
                    variants_writer.writerow(variant)
                
                for sample_variant in sample_variants_batch:
                    sample_variants_writer.writerow(sample_variant)
                
                total_variants += len(variants_batch)
                batch_count += 1
        
        # Generate samples data
        import datetime
        now = datetime.datetime.utcnow().isoformat()
        
        for sample_id, variant_count in sample_variant_counts.items():
            samples_writer.writerow([
                sample_id,
                f"Sample {sample_id}",
                f"Imported from {file_name}",
                "",  # owner_id
                "",  # group_id
                "false",  # is_public
                "",  # patient_id
                "",  # sample_type
                "",  # collection_date
                "processed",  # status
                "not_reviewed",  # review_status
                "{}",  # metadata
                "",  # clinical_notes
                "[]",  # phenotype_terms
                variant_count,  # variant_count
                now,  # created_at
                now,  # updated_at
                "",  # last_accessed
                "0",  # access_count
                "none",  # view_status
                "1"  # tier_level
            ])
        
        # Upload results to Cloud Storage
        output_bucket_obj = storage_client.bucket(output_bucket)
        
        # Upload variants file
        variants_blob = output_bucket_obj.blob(f"{output_prefix}{base_name}_variants.csv.gz")
        variants_buffer.seek(0)
        with io.BytesIO() as compressed:
            import gzip
            with gzip.GzipFile(fileobj=compressed, mode='wb') as f:
                f.write(variants_buffer.getvalue().encode('utf-8'))
            compressed.seek(0)
            variants_blob.upload_from_file(compressed, content_type='application/gzip')
        
        # Upload sample variants file
        sample_variants_blob = output_bucket_obj.blob(f"{output_prefix}{base_name}_sample_variants.csv.gz")
        sample_variants_buffer.seek(0)
        with io.BytesIO() as compressed:
            import gzip
            with gzip.GzipFile(fileobj=compressed, mode='wb') as f:
                f.write(sample_variants_buffer.getvalue().encode('utf-8'))
            compressed.seek(0)
            sample_variants_blob.upload_from_file(compressed, content_type='application/gzip')
        
        # Upload samples file (not compressed for easier editing)
        samples_blob = output_bucket_obj.blob(f"{output_prefix}{base_name}_samples.csv")
        samples_buffer.seek(0)
        samples_blob.upload_from_string(samples_buffer.getvalue(), content_type='text/csv')
        
        return {
            "success": True,
            "input_file": f"gs://{bucket_name}/{blob_path}",
            "output_files": {
                "variants": f"gs://{output_bucket}/{output_prefix}{base_name}_variants.csv.gz",
                "sample_variants": f"gs://{output_bucket}/{output_prefix}{base_name}_sample_variants.csv.gz",
                "samples": f"gs://{output_bucket}/{output_prefix}{base_name}_samples.csv"
            },
            "stats": {
                "total_variants": total_variants,
                "total_samples": len(sample_variant_counts),
                "batches_processed": batch_count
            }
        }
    
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_path):
            os.unlink(temp_path)

@functions_framework.http
def lookup_variant(request):
    """
    Cloud Function to look up a variant by its VRS ID.
    
    Expected request format:
    {
        "vrs_id": "ga4gh:VA:7:v9TQXvNOQeG1vNRVJCWlD_a1tRf_m2AP"
    }
    
    Returns:
        JSON response with variant information
    """
    # Parse request
    request_json = request.get_json(silent=True)
    
    if not request_json:
        return json.dumps({"error": "No JSON data provided"}), 400
    
    vrs_id = request_json.get("vrs_id")
    if not vrs_id:
        return json.dumps({"error": "No vrs_id specified"}), 400
    
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
        variants = [dict((k, v) for k, v in row.items()) for row in results]
        
        if not variants:
            return json.dumps({"message": f"No variant found for VRS ID: {vrs_id}"}), 404
        
        return json.dumps({"variants": variants}), 200
        
    except ValueError as e:
        return json.dumps({"error": str(e)}), 400
    except Exception as e:
        return json.dumps({"error": f"Internal error: {str(e)}"}), 500

# For local testing
if __name__ == "__main__":
    # Test VRS generation
    vrs_id = generate_vrs_id("chr7", 55174772, "GGAATTAAGAGAAGC", "", assembly="GRCh38")
    print(f"Generated VRS ID: {vrs_id}")
    
    # Test VRS parsing
    components = parse_vrs_id(vrs_id)
    print(f"Parsed components: {components}")
    
    # Test chromosome extraction
    chromosome = get_chromosome_from_vrs_id(vrs_id)
    print(f"Chromosome: {chromosome}")

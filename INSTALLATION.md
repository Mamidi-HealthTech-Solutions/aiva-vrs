# VRS ID Generator - Installation Guide

This guide explains how to install and use the VRS ID Generator package in both local and cloud environments.

## Local Installation

### Method 1: Install from local directory

```bash
# Clone the repository
git clone https://github.com/Mamidi-HealthTech-Solutions/aiva-vrs.git
cd aiva-vrs

# Install in development mode
pip install -e .
```

### Method 2: Install from GitHub (once published)

```bash
pip install git+https://github.com/Mamidi-HealthTech-Solutions/aiva-vrs.git
```

### Method 3: Install from PyPI (once published)

```bash
pip install aiva-vrs
```

## Using with OpenCRAVAT Processing Script

### Local Usage

Update your `generate_sample_csvs.py` script to use the package:

```python
# Import from the package
from aiva_vrs import generate_vrs_id, normalize_chromosome

def process_opencravat_csv(csv_path, output_dir, assembly='GRCh38', compress=True):
    # ...
    
    # Generate VRS ID
    vrs_id = generate_vrs_id(chrom, pos, ref, alt, assembly)
    
    # ...
```

### Cloud Environment Setup

#### Google Cloud Functions

1. Create a `requirements.txt` file:

```
aiva-vrs==0.1.0
google-cloud-storage==2.9.0
google-cloud-bigquery==3.3.5
functions-framework==3.3.0
```

2. Create a `main.py` file based on the example in `examples/cloud_function_example.py`

3. Deploy the function:

```bash
gcloud functions deploy process_variants \
  --runtime python39 \
  --trigger-http \
  --allow-unauthenticated \
  --source=. \
  --entry-point=process_variants
```

#### Using with Cloud Run

1. Create a `Dockerfile`:

```dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "process_variants.py"]
```

2. Build and deploy:

```bash
gcloud builds submit --tag gcr.io/your-project/vrs-processor
gcloud run deploy vrs-processor --image gcr.io/your-project/vrs-processor
```

## Integrating with Existing Projects

### Option 1: Direct Import

If you've installed the package:

```python
from aiva_vrs import generate_vrs_id, parse_vrs_id
```

### Option 2: Fallback Import

For maximum compatibility:

```python
try:
    # Try to import from installed package
    from aiva_vrs import generate_vrs_id, parse_vrs_id
    print("Successfully imported VRS generator from package")
except ImportError:
    # Fallback to local implementation
    try:
        from annotation_processor.utils.vrs.vrs_generator import generate_vrs_id, parse_vrs_id
        print("Successfully imported VRS generator from annotation_processor")
    except ImportError:
        # Second fallback
        from utils.vrs.vrs_generator import generate_vrs_id, parse_vrs_id
        print("Successfully imported VRS generator from utils")
```

## Performance Considerations

### Local Environment

For local processing, the default implementation is optimized for standard workloads.

### Cloud Environment

For cloud environments, consider these optimizations:

1. **Batch Processing**: Process variants in batches (see `examples/cloud_function_example.py`)
2. **Memory Management**: Use in-memory buffers and flush periodically
3. **Parallel Processing**: For large files, consider parallel processing with Cloud Run

## Troubleshooting

### Import Issues

If you encounter import errors:

```python
import sys
import os

# Add package directory to path (for development)
sys.path.append('/path/to/aiva-vrs')

# Now import should work
from aiva_vrs import generate_vrs_id
```

### Version Conflicts

If you encounter version conflicts:

```bash
# Create a virtual environment
python -m venv venv
source venv/bin/activate

# Install specific version
pip install aiva-vrs==0.1.0
```

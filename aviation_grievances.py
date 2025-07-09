import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source
from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.destinations import filesystem
import os
import argparse
from typing import Optional, List, Tuple
from datetime import datetime, date, timezone
import logging
import re

# ----------------- Logging Setup -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ----------------- Config & Constants -----------------
API_KEY = "579b464db66ec23bdd0000014f735595b38143ab7db9e9300fb4d49a"

API_URL = "https://api.data.gov.in/"
RAW_DATA_DIR = os.path.join("storage")
RESOURCE_NAME = "aviation_grievances_api"

def to_snake_case(name):
    """Convert camelCase to snake_case"""
    # Handle sequences of uppercase letters
    s1 = re.sub('([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    # Handle transitions from lowercase to uppercase
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.lower()

def standardize_column_names(item):
    """Standardize all column names to snake_case"""
    if isinstance(item, dict):
        standardized_item = {}
        for key, value in item.items():
            # Convert column name to snake_case
            snake_case_key = to_snake_case(key)
            standardized_item[snake_case_key] = value
        return standardized_item
    return item

import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source
from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.destinations import filesystem
import os
import argparse
from typing import Optional, List, Tuple
from datetime import datetime, date, timezone
import logging
import re
import requests

# ----------------- Logging Setup -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ----------------- Config & Constants -----------------
API_KEY = "579b464db66ec23bdd0000014f735595b38143ab7db9e9300fb4d49a"

API_URL = "https://api.data.gov.in/"
RAW_DATA_DIR = os.path.join("storage")
RESOURCE_NAME = "aviation_grievances_api"

def to_snake_case(name):
    """Convert camelCase to snake_case"""
    # Handle sequences of uppercase letters
    s1 = re.sub('([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    # Handle transitions from lowercase to uppercase
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.lower()

def standardize_column_names(item):
    """Standardize all column names to snake_case"""
    if isinstance(item, dict):
        standardized_item = {}
        for key, value in item.items():
            # Convert column name to snake_case
            snake_case_key = to_snake_case(key)
            standardized_item[snake_case_key] = value
        return standardized_item
    return item

@dlt.resource(name=RESOURCE_NAME, write_disposition="replace")
def aviation_grievances_custom():
    """Custom resource to handle updated_at from response metadata"""
    url = f"{API_URL}resource/7be93611-4e76-4077-8d00-6232d01367cf"
    
    offset = 0
    limit = 10
    max_offset = 50
    
    while offset <= max_offset:
        params = {
            'api-key': API_KEY,
            'format': 'json',
            'offset': offset,
            'limit': limit
        }
        
        response = requests.get(url, params=params)
        data = response.json()
        
        # Extract updated_date from response metadata
        updated_date = data.get('updated_date')
        
        # Process each record
        records = data.get('records', [])
        if not records:
            break
            
        for record in records:
            # Add updated_at from response metadata
            record['updated_at'] = updated_date
            
            # Add inserted_at timestamp
            record['inserted_at'] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            
            # Standardize column names
            record = standardize_column_names(record)
            
            yield record
        
        offset += limit
        
        # Check if we've reached the end
        if len(records) < limit:
            break

def get_kcc_source(max_offset: Optional[int] = 50):
    """Return a source with the custom resource"""
    return aviation_grievances_custom()

def add_timestamp(item):
    """Add inserted_at timestamp"""
    item["inserted_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return item

def process_item(item):
    """Combined processing function for timestamp and column standardization"""
    # First add timestamp
    item = add_timestamp(item)
    
    # Then standardize column names
    item = standardize_column_names(item)
    
    return item

#def parse_args() -> argparse.Namespace:
#    parser.add_argument("--max-offset", type=int, default=50000,
#                        help="Maximum pagination offset")
#    return parser.parse_args()

def main():
    #args = parse_args()

    # Create the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="aviation_grievances",
        destination="bigquery",
        dataset_name="aviation_grievances_data",
        progress='log'
    )

    today = date.today()
    logger.info(f"Running for: {today}")

    # Use the custom resource directly
    source = get_kcc_source()
    
    load_info = pipeline.run(source, loader_file_format="parquet")
    logger.info(f"Last Trace: {pipeline.last_trace}")

    # Get the readable dataset from the pipeline
    dataset = pipeline.dataset()

    return dataset.aviation_grievances_api.df()

if __name__ == "__main__":
    df = main()
    df.head()
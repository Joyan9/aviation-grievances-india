import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source
from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.destinations.adapters import bigquery_adapter
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
API_KEY = dlt.secrets.get("sources.aviation_grievances.api_key")

API_URL = "https://api.data.gov.in/"
API_ENDPOINT = "resource/7be93611-4e76-4077-8d00-6232d01367cf"
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

@dlt.resource(
    name=RESOURCE_NAME, 
    columns=[
        {"name": "inserted_date", "data_type": "date"}
    ],
    write_disposition="append",
    table_name="aviation_grievances_api"
)
def aviation_grievances_resource():
    """Custom resource to handle updated_at from response metadata"""
    url = f"{API_URL}{API_ENDPOINT}"
    
    offset = 0
    limit = 100
    max_offset = 5000
    
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
            
            # Add inserted_date as a string in YYYY-MM-DD format for partitioning
            record['inserted_date'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            
            # Standardize column names
            record = standardize_column_names(record)
            
            yield record
        
        offset += limit
        
        # Check if we've reached the end
        if len(records) < limit:
            break

def main():
    # Create the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="aviation_grievances",
        destination="bigquery",
        dataset_name="aviation_grievances_data",
        progress="log"
    )

    today = date.today()
    logger.info(f"Running for: {today}")

    # Apply column options
    data_source = bigquery_adapter(aviation_grievances_resource, partition="inserted_date")
    
    load_info = pipeline.run(data_source)
    logger.info(f"Last Trace: {pipeline.last_trace}")

    # Get the readable dataset from the pipeline
    dataset = pipeline.dataset()

    return dataset.aviation_grievances_api.df(), pipeline.last_trace

if __name__ == "__main__":
    df, last_trace = main()
    df.head()
    print(f"Last Trace: {last_trace}")

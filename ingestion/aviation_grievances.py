import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source
from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.destinations import filesystem
import os
import argparse
from typing import Optional, List, Tuple
from datetime import datetime, date
import logging

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

def get_kcc_source(
                  max_offset: Optional[int] = 50 
                  ):
    # Configure authentication
    auth = APIKeyAuth(
        name="api-key", 
        api_key=API_KEY, 
        location="query"
    )
    
    # Set up paginator with optional max offset
    paginator_config = {
        "limit": 10,
        "total_path": None,
        "offset_param": 'offset',
        "limit_param": 'limit',
    }
    
    # Add maximum_offset if provided
    if max_offset:
        paginator_config["maximum_offset"] = max_offset
    
    paginator = OffsetPaginator(**paginator_config)
    
    # Configure the REST API source
    config: RESTAPIConfig = {
        "client": {
            "base_url": API_URL,
            "auth": auth,
            "paginator": paginator
        },
        
        "resources": [
            {
                "name": RESOURCE_NAME,
                "endpoint": {
                    "path": "resource/7be93611-4e76-4077-8d00-6232d01367cf",
                    "params": {
                        'format': 'json'
                    }
                }
            }
        ]
    }
    
    # Create and return the source
    return rest_api_source(config)


def parse_args() -> argparse.Namespace:
    parser.add_argument("--max-offset", type=int, default=50000,
                        help="Maximum pagination offset")
    return parser.parse_args()

def main():
    #args = parse_args()
    
    # Create the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="aviation_grievances",
        destination="duckdb",
        dataset_name="daily_data",
        #progress='log'
    )
    
    today = date.today()
    logger.info(f"Running for: {today}")
    
    source = get_kcc_source()
    load_info = pipeline.run(source, loader_file_format="parquet")
    logger.info(f"Last Trace: {pipeline.last_trace}")

    # Get the readable dataset from the pipeline
    dataset = pipeline.dataset()

    # print the row counts of all tables in the destination as dataframe
    print(dataset.row_counts().df())
if __name__ == "__main__":
    main()
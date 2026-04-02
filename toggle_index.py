#!/usr/bin/env python3
"""
Appwrite Index Toggle Script

This script toggles an index in an Appwrite database table:
- If the index exists → delete it
- If the index does not exist → create it

Required environment variables:
    APPWRITE_PROJECT_ID
    APPWRITE_API_KEY
    APPWRITE_DATABASE_ID
    APPWRITE_TABLE_ID
    APPWRITE_INDEX_KEY
    APPWRITE_ATTRIBUTE
"""

import os
import sys
import logging
import requests
from datetime import date

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_required_env_vars():
    """Get required environment variables."""
    env_vars = {
        'project_id': os.getenv('APPWRITE_PROJECT_ID'),
        'api_key': os.getenv('APPWRITE_API_KEY'),
        'database_id': os.getenv('APPWRITE_DATABASE_ID'),
        'table_id': os.getenv('APPWRITE_TABLE_ID'),
        'index_key': os.getenv('APPWRITE_INDEX_KEY'),
        'attribute': os.getenv('APPWRITE_ATTRIBUTE'),
    }
    
    missing = [key for key, value in env_vars.items() if not value]
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)
    
    return env_vars


def check_index_exists(base_url, headers, database_id, collection_id, index_key):
    """Check if an index exists by trying to get it."""
    url = f"{base_url}/databases/{database_id}/collections/{collection_id}/indexes/{index_key}"
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            return False
        else:
            logger.warning(f"Unexpected status code when checking index: {response.status_code}")
            return False
    except requests.RequestException as e:
        logger.error(f"Error checking if index exists: {e}")
        raise


def create_index(base_url, headers, database_id, collection_id, index_key, attribute):
    """Create an index in the Appwrite collection."""
    url = f"{base_url}/databases/{database_id}/collections/{collection_id}/indexes"
    
    payload = {
        "key": index_key,
        "type": "key",
        "attributes": [attribute]
    }
    
    logger.info(f"Creating index '{index_key}' on attribute '{attribute}'...")
    
    response = requests.post(url, json=payload, headers=headers, timeout=30)
    
    logger.info(f"API response status: {response.status_code}")
    
    if response.status_code in [200, 201, 202]:
        logger.info("Index created successfully!")
        return True
    else:
        logger.error(f"Failed to create index. Status code: {response.status_code}")
        logger.error(f"Response body: {response.text}")
        return False


def delete_index(base_url, headers, database_id, collection_id, index_key):
    """Delete an index from the Appwrite collection."""
    url = f"{base_url}/databases/{database_id}/collections/{collection_id}/indexes/{index_key}"
    
    logger.info(f"Deleting index '{index_key}'...")
    
    response = requests.delete(url, headers=headers, timeout=30)
    
    logger.info(f"API response status: {response.status_code}")
    
    if response.status_code in [200, 204]:
        logger.info("Index deleted successfully!")
        return True
    else:
        logger.error(f"Failed to delete index. Status code: {response.status_code}")
        logger.error(f"Response body: {response.text}")
        return False


def main():
    """Main function to toggle the index."""
    # Get current ISO week number for logging purposes
    current_week = date.today().isocalendar()[1]
    logger.info(f"Current ISO week number: {current_week}")
    
    today = date.today().weekday()  # Monday=0, Thursday=3
    if today not in (0, 3):
        logger.info("Today is not Monday or Thursday. Skipping execution.")
        sys.exit(0)
        
    # Get environment variables
    env = get_required_env_vars()
    
    # Appwrite API configuration
    base_url = "https://cloud.appwrite.io/v1"
    headers = {
    "Content-Type": "application/json",
    "X-Appwrite-Project": env['project_id'],
    "X-Appwrite-Key": env['api_key']
    }
    
    database_id = env['database_id']
    collection_id = env['table_id']  # In Appwrite v2+, tables are called collections
    index_key = env['index_key']
    attribute = env['attribute']
    
    logger.info(f"Target: Database={database_id}, Collection={collection_id}, Index={index_key}")
    
    try:
        # Check if index exists
        index_exists = check_index_exists(base_url, headers, database_id, collection_id, index_key)
        
        if index_exists:
            logger.info("Index exists - Action: DELETE")
            success = delete_index(base_url, headers, database_id, collection_id, index_key)
        else:
            logger.info("Index does not exist - Action: CREATE")
            success = create_index(base_url, headers, database_id, collection_id, index_key, attribute)
        
        if not success:
            sys.exit(1)
            
        logger.info("Toggle operation completed successfully!")
        
    except requests.RequestException as e:
        logger.error(f"Request failed: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

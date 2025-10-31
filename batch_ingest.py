# cuehling
'''
This sends my data to the local S3 file.Batch
'''

import s3fs
from s3fs.core import S3FileSystem
#import numpy as np
import requests


API_KEY = "fewuBc8rLWBReMOEqUrPYPiISeW5dcul4aP6cmbc"
BASE_URL = "https://eonet.gsfc.nasa.gov/api/v3"

def ingest_data():
    
    data = fetch_events(limit=5, days=1, status="open")

    s3 = S3FileSystem()
    # S3 bucket directory
    DIR = 's3://ece5984-s3-cuehling/Global_Natural_Disaster_Monitoring/initial_data'                    # insert your S3 URI here
    # Push data to S3 bucket as a pickle file
    with s3.open('{}/{}'.format(DIR, 'initial_data.pkl'), 'wb') as f:
        f.write(data)


def fetch_events(limit=5, days=20, status="open", source=None):
    params = {
        "limit": limit,
        "days": days,
        "status": status,
    }
    if source:
        params["source"] = source
    if API_KEY:
        params["api_key"] = API_KEY

    url = f"{BASE_URL}/events"
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# if __name__ == "__main__":
#     print(fetch_events(limit=5, days=1, status="open")) #, source="InciWeb")
import gcsfs

import datetime
import logging
import os
import requests

import numpy as np
import pandas as pd

from datetime import datetime, timedelta

#from logging.config import dictConfig
#from src.log_config import log_config

#dictConfig(log_config)
#logger = logging.getLogger("polygonio-news-sentiment-model-data-logger")


# TODO: Load Polygon News dataframe from GCS
def gcs_read_dataframe(gcs_path):
    pass

# TODO: Write Dataframe back to GCS
def gcs_write_dataframe(gcs_path):
    pass

# TODO: Write current dataframe to backup bucket/folder
def gcs_backup_dataframe(gcs_path):
    pass

# TODO: Query for max(timestamp)
def polygonio_max_time_stamp(df):
    pass

# TODO: Query Polygon News API for new data (based on max(timestamp))
def polygonio_query_news_api():
    pass

# TODO: Update Dataframe

    

if __name__ == '__main__':

    fs = gcsfs.GCSFileSystem(project='mlops-3')
    gcs_path = "gs://polygonio-news-sentiment-test/data/polygonio_news_data.pkl"
    current_timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    gcs_backup_path = "gs://polygonio-news-sentiment-test/data/backup/polygonio_news_data_" + current_timestamp + ".pkl"
    df_columns = ["amp_url", "article_url", "author", "description", "id", "image_url", "keywords", "published_utc", "publisher.favicon_url", 
                    "publisher.homepage_url", "publisher.logo_url", "publisher.name", "tickers", "title"]

    # Check if pickle file exists
    # if exists - call max(timestamp)
    # if not exists - max(timestamp) = previous day
    if fs.exists(gcs_path):
        # download it
        df_current = pd.read_pickle(gcs_path)
        # TODO: What is the right string format?
        max_published_utc = df_current["published_utc"].max()
        print(f"MAX_PUBLISHED_UTC (Exists): {max_published_utc}")
    else:
        df_current = pd.DataFrame(columns=df_columns)
        max_published_utc = (datetime.now() - timedelta(days = 10)).strftime('%Y-%m-%dT23:59:59Z')
        print(f"MAX_PUBLISHED_UTC (DNE): {max_published_utc}")

    # Call news API and get data based on max(timestamp)
    api_key = os.getenv("POLYGON_API_KEY")
    # TODO: remove the limit when pushing to prod
    next_url = f"https://api.polygon.io/v2/reference/news?published_utc.gt={max_published_utc}&limit=1000&apiKey={api_key}"
    print(f"NEXT_URL: {next_url}")
    news = []
    count = 0

    while next_url:
        resp = requests.get(next_url)
        if resp.ok:
            news += resp.json()
            if "count" in resp.json().keys():
                count += resp.json()["count"]
            print(count)
            if "next_url" in resp.json().keys():
                next_url = resp.json()["next_url"] + f"&apiKey={api_key}"
                print(next_url)
            else:
                #df_new = pd.DataFrame(resp.json()['results'])\
                df_new = pd.json_normalize(resp.json()['results'], max_level=1)
                print(df_new.columns)
                print(df_new.head())
                df_new.to_csv("./testing.csv")
                print(f"NEW COLUMNS: {df_new.columns}")
                # Process and clean data
                df_new['sentiment'] = np.nan
                df_new['sentiment_score'] = np.nan
                df_new.fillna('', inplace=True)
                break
        else:
            # Create empty df_new
            df_new = pd.DataFrame(columns=df_columns)
            print(f"Request failed with {resp.status_code}")
            break
    
    # Call model server
    # Update each new article
    # Add new data to existing data
    df_updated = pd.concat([df_current, df_new])

    # Backup existing data on GCS
    if fs.exists(gcs_path):
        fs.copy(gcs_path,gcs_backup_path)
    else:
        print("Not able to back up news data")

    # Write data to GCS
    df_updated.to_pickle("./test_pickle.pkl")
    df_updated.to_pickle(gcs_path)

    print(f"CURRENT: {len(df_current.index)}")
    print(f"CURRENT COLUMNS: {df_current.columns}")
    print(f"NEW: {len(df_new.index)}")
    print(f"NEW COLUMNS: {df_new.columns}")
    print(f"UPDATED: {len(df_updated.index)}")
    print(f"UPDATED COLUMNS: {df_updated.columns}")
    #print(df_updated.head())

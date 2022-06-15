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

def query_sentiment_api():
    sentiment_url = f"https://polygonio-news-sentiment-data-v2-d3zpexdhjq-uc.a.run.app/sentiment"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }
    data = {
        "headline": "this is a test of the emergency broadcast systme",
    }

    resp = requests.post(sentiment_url, headers=headers, data=json.dumps(data))

    return df

    

if __name__ == '__main__':

    fs = gcsfs.GCSFileSystem(project='mlops-3')
    gcs_path = "gs://polygonio-news-sentiment-test/data/polygonio_news_data.pkl"
    current_timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    gcs_backup_path = "gs://polygonio-news-sentiment-test/data/backup/polygonio_news_data_" + current_timestamp + ".pkl"
    df_columns = ["amp_url", "article_url", "author", "description", "id", "image_url", "keywords", "published_utc", "publisher.favicon_url", 
                    "publisher.homepage_url", "publisher.logo_url", "publisher.name", "sentiment", "sentiment_socre", "tickers", "title"]

    # Check if pickle file exists
    # if exists: call max(timestamp)
    # if not exists: max(timestamp) = previous day
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
    api_url = f"https://api.polygon.io/v2/reference/news?published_utc.gt={max_published_utc}&limit=1000&apiKey={api_key}"
    print(f"NEXT_URL: {api_url}")
    news = []
    count = 0

    # Grab the initial payload from the API
    resp = requests.get(api_url)
    if resp.ok and resp.json()["results"]:
        count += resp.json()["count"]
        print(f"If Count: {count}")
        news += resp.json()["results"]
        print(f"If News: {len(news)}\n\n")

        while "next_url" in resp.json().keys():
            api_url = resp.json()["next_url"] + f"&apiKey={api_key}"
            resp = requests.get(api_url)
            if resp.ok and resp.json()["results"]:
                news += resp.json()["results"]
                print(f"While News: {len(news)}")
                count += resp.json()["count"]
                print(f"While Count: {count}")
                print(api_url)
            else:
                print("BREAK")
                break

        # Create dataframe will full payload
        df_new = pd.json_normalize(news, max_level=1)
        # Process and clean data
        df_new['sentiment'] = np.nan
        df_new['sentiment_score'] = np.nan
        df_new.fillna('', inplace=True)

        sentiment_url = f"https://polygonio-news-sentiment-data-v2-d3zpexdhjq-uc.a.run.app/sentiment"
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
        }
        data = {
            "headline": "this is a test of the emergency broadcast systme",
        }
        resp = requests.post(sentiment_url, headers=headers, data=json.dumps(data))
        
        df['a'] = df['a'].map(lambda a: a / 2.)
        result = [f(x) for x in df['col']]

    else:
        # Create empty dataframe
        df_new = pd.DataFrame(columns=df_columns)
        print(f"Request failed with {resp.status_code}")
  
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
    df_updated.to_pickle(gcs_path)

    print(f"CURRENT: {len(df_current.index)}")
    print(f"CURRENT COLUMNS: {df_current.columns}")
    print(f"NEW: {len(df_new.index)}")
    print(f"NEW COLUMNS: {df_new.columns}")
    print(f"UPDATED: {len(df_updated.index)}")
    print(f"UPDATED COLUMNS: {df_updated.columns}")

    df_current.to_csv("./df_current.csv")
    df_new.to_csv("./df_new.csv")
    df_updated.to_csv("./df_updated.csv")
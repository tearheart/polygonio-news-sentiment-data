from fastapi import FastAPI

import logging
from logging.config import dictConfig
from log_config import log_config # this is your local file

dictConfig(log_config)
logger = logging.getLogger("polygonio-news-sentiment-logger")

app = FastAPI()

@app.get("/health")
def health():
    logger.info("It worked")
    return {"message": "Hello World"}

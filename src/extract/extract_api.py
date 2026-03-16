import requests
from pathlib import Path
import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from logger import build_logger
import time 

BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

LOG_PATH = BASE_DIR / "logs" / "etl-run.log"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://jsonplaceholder.typicode.com"
ENDPOINTS = {
    "users" : "/users",
    "posts" : "/posts",
    "comments" : "/comments"
}

logger = build_logger(LOG_PATH)

def fetch_json(url: str, endpoint_name: str):
    logger.info("Fetching into %s", url)
    start = time.time()
    try:
        r = requests.get(url=url, timeout=10)
    except Exception as e:
        logger.exception("Exception occured while fetching data to %s. Error: %s", url, e)
        raise
    r.raise_for_status()
    end = time.time() - start
    
    data = r.json()
    
    rows_count = len(data) #sekarang ok, tapi jadi masalah kalau struktur nya ada status, code, message, baru data
    
    logger.info(f"Fetched {endpoint_name} endpoint | rows = {rows_count} | duration = {end:.2f}s")
    return data

def save_json(data, path: Path):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.exception("Exception occured saving data to %s. Error: %s", path, e)
        raise
    
def extract_all():
    for k, v in ENDPOINTS.items():
        r = fetch_json(BASE_URL + v, k)
        file_name = k + ".json"
        save_json(r, RAW_DIR / file_name)
        logger.info("Success, saved %s to %s", k, RAW_DIR / file_name)
    
if __name__ == "__main__":
    logger.info("="*90)
    logger.info("ETL Start")
    extract_all()
    logger.info("ETL Finished")
    logger.info("="*90)
    
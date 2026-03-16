import requests
import pandas as pd
from pathlib import Path
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from logger import build_logger
import time 

BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSDED_DIR = BASE_DIR / "data" / "processed"

LOG_PATH = BASE_DIR / "logs" / "etl-run.log"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSDED_DIR.mkdir(parents=True, exist_ok=True)

logger = build_logger(LOG_PATH)

def fetch_json(url: str):
    logger.info("Fetching into %s", url)
    start = time.time()
    r = requests.get(url=url, timeout=10)
    r.raise_for_status()
    end = time.time() - start
    logger.info("Success fecth %s for %ds", url, end)
    return r

def save_json(data, path: Path):
    df = pd.DataFrame(data)
    df.to_json(path, orient="records")
    
def extract_all():
    users = fetch_json("https://jsonplaceholder.typicode.com/users")
    posts = fetch_json("https://jsonplaceholder.typicode.com/posts")
    comments = fetch_json("https://jsonplaceholder.typicode.com/comments")
    
    save_json(users, BASE_DIR / "users.json")
    logger.info("Success, saved users to %s", BASE_DIR / "users.json")
    save_json(posts, BASE_DIR / "posts.json")
    logger.info("Success, saved posts to %s", BASE_DIR / "posts.json")
    save_json(comments, BASE_DIR / "comments.json")
    logger.info("Success, saved comments to %s", BASE_DIR / "comments.json")
    
if __name__ == "__main__":
    extract_all()
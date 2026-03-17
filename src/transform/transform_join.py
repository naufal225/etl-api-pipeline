import pandas as pd
from pathlib import Path
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from logger import build_logger

BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"

PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

LOG_PATH = BASE_DIR / "logs" / "etl-run.log"

logger = build_logger(LOG_PATH)

DATA = {
    "users":"users.json",
    "posts":"posts.json",
    "comments":"comments.json"
}

def load_data(path: Path):
    try:
        logger.info("Start to load into %s", path)
        
        df = pd.read_json(path)
        
        logger.info("%s loaded successfully", path)
        
        return df
    except Exception as e:
        logger.error("Error occured while load file %s", path)
        raise
    
def normalize(df: pd.DataFrame, cols: dict) -> pd.DataFrame:
    return df.rename(columns=cols)
    

def join_data(df1: pd.DataFrame, df2: pd.DataFrame, on:str, how:str="inner") -> pd.DataFrame:
    try:
        return df1.merge(df2, how=how, on=on)
    except Exception as e:
        logger.exception("Error occured while join dataframe: %s", e)
        raise

def transform():
    df_users = load_data(RAW_DIR / "users.json")
    df_posts = load_data(RAW_DIR / "posts.json")
    df_comments = load_data(RAW_DIR / "comments.json")
    
    df_users = normalize(df_users, {
        "id":"user_id",
        "name":"user_name",
        "email":"user_email"
    })
    
    df_posts = normalize(df_posts, {
        "id":"post_id",
        "userId":"user_id",
        "body": "post_body"
    })
    
    df_comments = normalize(df_comments, {
        "postId":"post_id",
        "body":"comment_body"
    })
    
    comment_count = (
        df_comments.groupby("post_id")
            ["id"].count()
            .reset_index(name="comment_count")
    )
    
    df_joined = join_data(df_posts, df_users, on="user_id", how="left")
    df_joined = join_data(df_joined, comment_count, on="post_id", how="left")
    
    df_joined["title_length"] = df_joined["title"].str.len()
    df_joined["post_body_length"] = df_joined["post_body"].str.len()
    
    avg_title_length = df_joined["title_length"].mean()
    avg_post_body_length = df_joined["post_body_length"].mean()
    
    metrics = pd.DataFrame([{
        "avg_title_length" : avg_title_length,
        "avg_post_body_length": avg_post_body_length
    }])
    
    try:
        assert df_joined["post_id"].isna().sum() == 0
        assert df_joined["user_id"].isna().sum() == 0
    except AssertionError as e:
        logger.exception("AssertionError exception occured: %s", e)
        raise
    
    try:
        path = PROCESSED_DIR / "posts_analytics.csv"
        logger.info("Starting save to %s", path)
        df_joined.to_csv(path, index=False)
        logger.info("Successfully saved to %s", path)
        
    except Exception as e:
        logger.exception("Error occured while saving to %s, Error: %s", path, e)
        
    try:
        path = PROCESSED_DIR / "metrics.csv"
        logger.info("Starting save to %s", path)
        metrics.to_csv(path, index=False)
        logger.info("Successfully saved to %s", path)
        
    except Exception as e:
        logger.exception("Error occured while saving to %s, Error: %s", path, e)
        
def main():
    logger.info("="*90)
    logger.info("Transform Start")
    transform()
    logger.info("Transform Finished")
    
if __name__ == "__main__":
    main()
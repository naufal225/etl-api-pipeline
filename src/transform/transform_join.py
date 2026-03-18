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

def load_data(path: Path):
    try:
        logger.info("Start to load into %s", path)
        
        df = pd.read_json(path)
        
        logger.info("%s loaded successfully", path)
        
        return df
    except Exception as e:
        logger.exception("Failed loading dataset %s", path)
        raise
    
def validate(df: pd.DataFrame):
    missing_post = df["post_id"].isna().sum()
    missing_user = df["user_id"].isna().sum()
    missing_title = df["title"].isna().sum()
    
    if missing_post > 0 or missing_user > 0:
        raise ValueError(
            f"Data validation failed | missing post = {missing_post} | missing user = {missing_user}"
        )
        
    if missing_title > 0:
        raise ValueError(
            f"Data validation failed | missing title = {missing_title}"
        )
        
    if (df["comment_count"] < 0).any():
        raise ValueError(
            f"Data validation failed | comment count = {missing_title}"
        )

def transform():
    df_users = load_data(RAW_DIR / "users.json")
    df_posts = load_data(RAW_DIR / "posts.json")
    df_comments = load_data(RAW_DIR / "comments.json")
    
    df_users = df_users.rename(columns={
        "id":"user_id",
        "name":"user_name",
        "email":"user_email"
    })
    
    df_posts = df_posts.rename(columns= {
        "id":"post_id",
        "userId":"user_id",
        "body": "post_body"
    })
    
    df_comments = df_comments.rename(columns={
        "postId":"post_id",
        "body":"comment_body"
    })
    
    comment_count = (
        df_comments.groupby("post_id")
            .size()
            .reset_index(name="comment_count")
    )
    
    logger.info("Joining posts with users")
    df_joined = df_posts.merge(right=df_users, on="user_id", how="left")
    
    logger.info("Joining comments aggregation")
    df_joined = df_joined.merge(comment_count, on="post_id", how="left")
    
    df_joined["title_length"] = df_joined["title"].str.len()
    df_joined["post_body_length"] = df_joined["post_body"].str.len()
    df_joined["comment_count"] = df_joined["comment_count"].fillna(0).astype(int)
    
    avg_title_length = df_joined["title_length"].mean()
    avg_post_body_length = df_joined["post_body_length"].mean()
    
    metrics = pd.DataFrame([{
        "avg_title_length" : avg_title_length,
        "avg_post_body_length": avg_post_body_length
    }])
    
    df_final = df_joined[[
        "post_id",
        "user_id",
        "user_name",
        "title",
        "title_length",
        "comment_count"
    ]]
    
    try:
        validate(df_final)
    except ValueError as e:
        logger.exception(e)
        raise
    
    try:
        path = PROCESSED_DIR / "posts_analytics.csv"
        logger.info("Starting save to %s", path)
        df_final.to_csv(path, index=False)
        logger.info("Successfully saved to %s", path)
        
    except Exception as e:
        logger.exception("Error occured while saving to %s", path)
        
    try:
        path = PROCESSED_DIR / "metrics.csv"
        logger.info("Starting save to %s", path)
        metrics.to_csv(path, index=False)
        logger.info("Successfully saved to %s", path)
        
    except Exception as e:
        logger.exception("Error occured while saving to %s", path)
        
def main():
    logger.info("="*90)
    logger.info("Transform Start")
    transform()
    logger.info("Transform Finished")
    
if __name__ == "__main__":
    main()

"""
====================== ANALISIS  ======================

Status:
- Transformasi inti sudah ada, tetapi quality gate dan reliability-nya belum
  cukup kuat untuk data pipeline yang harus konsisten dari waktu ke waktu.

Gap utama:
1. Belum ada validasi schema input sebelum rename/join. Jika kolom API berubah,
   error baru muncul di tengah transform dan sulit ditelusuri.
2. Fungsi `validate()` punya bug pesan error: pada cek `comment_count < 0`,
   nilai yang dilaporkan justru `missing_title`.
3. Exception saat simpan CSV hanya dilog dan tidak di-raise. Ini berbahaya
   karena pipeline bisa terlihat sukses walau output gagal terbentuk.
4. Belum ada quality check untuk duplicate `post_id`, referential integrity
   `user_id`, dan anomali cardinality hasil join.
5. Belum ada kontrak schema output yang eksplisit, termasuk dtype, urutan
   kolom, dan batasan nullability.
6. `metrics.csv` sudah dibuat, tetapi belum dipakai sebagai bagian dari
   monitoring atau validation layer berikutnya.
7. Import masih memakai `sys.path.append`, yang menunjukkan packaging project
   belum clean untuk deployment atau test automation.

Agar lebih profesional:
- Tambahkan schema validation di awal transform.
- Fail pipeline ketika write output gagal.
- Tambahkan data quality checks yang eksplisit dan terukur.
- Kembalikan summary transform agar orchestration layer bisa merekam metrik.

==========================================================================
"""

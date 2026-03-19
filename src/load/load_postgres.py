import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import execute_values
from psycopg2.extensions import cursor
import dotenv
from dotenv import load_dotenv
import os
import sys
import time
import logging

from pathlib import Path
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import PgConfig

BASE_DIR = Path(__file__).resolve().parents[2]

PROCESSED_DIR = BASE_DIR / "data" / "processed"

logger = logging.getLogger(__name__)

def load_config() -> PgConfig:

    try:
        logger.info("Start to load config")

        load_dotenv()
        cfg = PgConfig.from_env()

        logger.info("Config loaded succeessfully: %s", cfg.safe_str())

        return cfg
    except Exception:
        logger.exception("Exception occured while loading config")
        raise

def load_csv(path: Path) -> pd.DataFrame:

    try:
        logger.info("Start to load csv file: %s", path)

        df = pd.read_csv(path)

        logger.info("Csv file loaded successfuly")

        return df
    except Exception:
        logger.exception("Exception occured while load csv file: %s", path)
        raise


def insert_upsert(cur: cursor, df: pd.DataFrame):
    cols = ["post_id", "user_id", "user_name", "title", "title_length", "comment_count"]
    rows = list(df[cols].itertuples(index=False, name=None))

    try:

        sql = f"""
                INSERT INTO post_analytics ({",".join(cols)})
                VALUES
                %s
                ON CONFLICT (post_id) DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    user_name = EXCLUDED.user_name,
                    title = EXCLUDED.title,
                    title_length = EXCLUDED.title_length,
                    comment_count = EXCLUDED.comment_count
            """

        execute_values(cur, sql, rows, page_size=1000)

        return len(rows)

    except Exception:
        logger.info("Exception occured while insert to database")
        raise


def load_into_postgres(path: Path, cfg: PgConfig):

    df = load_csv(path / "posts_analytics.csv")

    if len(df) == 0:
        logger.error("Csv file has 0 row")
        raise RuntimeError("Csv file has 0 row")

    try:
        logger.info("Start to create connection")

        conn = psycopg2.connect(
            host=cfg.host,
            port=cfg.port,
            dbname=cfg.dbname,
            user=cfg.user,
            password=cfg.password
        )

        conn.autocommit = False

        logger.info("Connection created successfully")

    except OperationalError:
        logger.exception("Incorrect credentials")
        raise
    except Exception:
        logger.exception("Exception occured while making connection")
        raise

    try:

        with conn.cursor() as cur:
            logger.info("Start to insert into database")

            start = time.time()

            len_data = insert_upsert(cur, df)
            conn.commit()

            end = time.time() - start

            logger.info("Insert %d rows into database | dur = %.4fs", len_data, end)
    except Exception:

        conn.rollback()
        logger.exception("Exception occured while inserting data into database, rollback changes...")
        raise

    finally:
        if conn:
            conn.close()
            logger.info("Database conncection closed")


# def main():
#     logger.info("Start to load into database")

#     start = time.time()
#     cfg = load_config()
#     load_into_postgres(PROCESSED_DIR, cfg)
#     end = time.time() - start

#     logger.info("Load into database finished | dur = %.4fs", end)


# if __name__ == "__main__":
#     main()


"""
====================== ANALISIS  ======================

Status:
- Layer load sudah punya fondasi yang paling matang di project ini karena
  sudah memakai UPSERT, transaction, dan logging dasar. Namun masih ada gap
  penting sebelum layak disebut ETL load layer yang production-oriented.

Gap utama:
1. `cfg.safe_str()` masih berpotensi membocorkan password ke log. Ini isu
   keamanan yang harus diprioritaskan.
2. `load_dotenv()` dipanggil di layer load, sehingga concern bootstrap config
   bercampur dengan concern database loading.
3. `insert_upsert()` mematerialisasi seluruh row ke memory:
   `list(df[cols].itertuples(...))`. Ini aman untuk data kecil, tetapi menjadi
   bottleneck saat volume membesar.
4. Belum ada validasi schema/dtype sebelum insert. Jika kolom CSV berubah,
   error baru akan muncul di database layer.
5. Belum ada retry untuk koneksi atau operasi insert, sehingga gangguan
   sementara pada jaringan/DB langsung menggagalkan job.
6. Logging sudah mencatat durasi total, tetapi belum ada observability per
   batch, total row target table setelah load, atau reconciliation summary.
7. Import masih memakai `sys.path.append`, yang menandakan struktur packaging
   belum siap untuk test runner atau deployment yang lebih rapi.

Agar lebih profesional:
- Redact credential dan pisahkan config bootstrap dari load logic.
- Tambahkan chunked upsert / batch iterator untuk dataset besar.
- Tambahkan pre-load schema validation dan post-load verification.
- Gunakan retry policy terbatas untuk failure yang bersifat sementara.
- Jadikan fungsi ini dipanggil dari orchestration layer, bukan dieksekusi
  manual sebagai script yang berdiri sendiri.

==========================================================================
"""

import requests
from pathlib import Path
import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
import time 

BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger(__name__)

print(logger)

BASE_URL = "https://jsonplaceholder.typicode.com"
ENDPOINTS = {
    "users" : "/users",
    "posts" : "/posts",
    "comments" : "/comments"
}

def fetch_json(
    url: str,
    endpoint_name: str,
    max_attempts: int = 3,
    base_delay: float = 1.0
):
    start = time.perf_counter()
    last_error = None    
    
    for attempt in range(1, max_attempts + 1):
        try:
            logger.info("Fetching endpoint=%s url=%s attempt:%d/%d",
                endpoint_name,
                url,
                attempt,
                max_attempts            
            )

            response = requests.get(url=url, timeout=10)
            response.raise_for_status()
            data = response.json()

            duration = time.perf_counter() - start
            rows_count = len(data) if isinstance(data, list) else 1

            logger.info("Fetched endpoint=%s rows=%d duration=%.2f",
                url,
                rows_count,
                duration
            )

            return data
        except requests.exceptions.Timeout as e:
            last_error = e
            logger.warning("Timeout while fething endpoint=%s attempt=%d/%d",
                endpoint_name,
                attempt,
                max_attempts               
            )
        except requests.exceptions.ConnectionError as e:
            last_error = e
            logger.warning("Connection error while fetching endpoint=%s attempt=%d/%d",
                endpoint_name,
                attempt,
                max_attempts               
            )
        except requests.exceptions.HTTPError as e:
            last_error = e
            status_code = e.response.status_code if e.response is not None else None
            
            if status_code is not None and status_code < 500 and status_code != 429:
                logger.exception("Non-retriable HTTP error endpoint=%s status=%s",
                    endpoint_name, 
                    status_code                 
                )
                raise
            
            logger.warning("Retriable HTTP error endpoint=%s status_code=%s attempt=%d/%d",
                endpoint_name,
                status_code,
                attempt,
                max_attempts
            )
        except requests.exceptions.RequestException as e:
            last_error = e
            
            logger.exception("Unexpected request error endpoint=%s attempt=%d/%d",
                endpoint_name,
                attempt,
                max_attempts
            )
            raise
        
        if attempt < max_attempts:
            sleep_seconds = base_delay * attempt
            logger.info("Retrying endpoint=%s in %.1f",
                endpoint_name,
                sleep_seconds
            )
    
    logger.exception(
        "Failed fetching endpoint=%s after %d attempts",
        endpoint_name,
        max_attempts,
        exc_info=last_error
    )
    raise RuntimeError(
        f"Failed fetching endpoint={endpoint_name} after {max_attempts} attempts"
    ) from last_error

def save_json(data, path: Path):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.exception("Exception occured saving data to %s. Error: %s", path, e)
        raise
    
def extract_all():
    for k, v in ENDPOINTS.items():
        r = fetch_json(f"{BASE_URL}{v}", k)
        file_name = k + ".json"
        save_json(r, RAW_DIR / file_name)
        logger.info("Success, saved %s to %s", k, RAW_DIR / file_name)
    return RAW_DIR
        
# def main():    
#     logger.info("="*90)
#     logger.info("ETL Start")
#     extract_all()
#     logger.info("ETL Finished")
    
# if __name__ == "__main__":
#     main()

"""
====================== ANALISIS ======================

Status:
- Layer extract sudah berjalan untuk use case belajar, tetapi belum cukup
  robust untuk API ingestion yang stabil di lingkungan industri.

Gap utama:
1. Retry belum benar-benar berjalan. Di dalam loop `for`, exception langsung
   di-raise pada kegagalan pertama, sehingga percobaan berikutnya tidak pernah
   dieksekusi.
2. Belum memakai `requests.Session`, backoff, dan retry berbasis status code
   (`429`, `500`, `502`, `503`, `504`).
3. Belum ada validasi schema response. Kode masih mengasumsikan bentuk JSON
   sederhana, padahal API produksi sering punya wrapper `status/code/data`.
4. Raw file ditulis langsung ke target path tanpa atomic write atau checksum,
   sehingga raw zone rawan korup jika proses terputus di tengah jalan.
5. Belum ada metadata extract seperti extraction timestamp, source endpoint,
   row count, response status, dan latency per endpoint.
6. Import masih mengandalkan `sys.path.append`, tanda bahwa package structure
   belum rapi untuk project Python yang reusable.

Agar lebih profesional:
- Gunakan `Session` reusable dengan retry + exponential backoff.
- Validasi response contract sebelum disimpan.
- Simpan metadata extract dan buat raw write yang atomic.
- Pisahkan concern HTTP client, persistence, dan orchestration stage.

==========================================================================
"""

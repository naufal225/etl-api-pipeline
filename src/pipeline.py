from extract.extract_api import extract_all
from transform.transform_join import transform
from load.load_postgres import load_into_postgres
from logger import build_logger
from config import PgConfig
from dotenv import load_dotenv
from pathlib import Path
import time

BASE_DIR = Path(__file__).resolve().parents[1]

LOG_PATH = BASE_DIR / "logs" / "etl-run.log"
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

logger = build_logger(LOG_PATH)

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

def main():
   logger.info("")
   logger.info("=" * 100)
   logger.info("PIPELINE START")
   start = time.time()
   
   cfg = load_config()
   
   try:
      logger.info("Starting to extract data from API")
      start_extract = time.time()
      
      extract_all()
      
      end_extract = time.time() - start_extract
      logger.info("Finished extra data from API | dur = %.4f", end_extract)
   except Exception:
      logger.exception("Exception occured while extract data on pipeline")
      raise      
   
   try:
      logger.info("Starting to transform data")
      start_transform = time.time()
      
      transform()
      
      end_transform = time.time() - start_transform
      logger.info("Finished transform data | dur = %.4f", end_transform)
   except Exception:
      logger.exception("Exception occured while transform data on pipeline")
      raise      
   
   try:
      logger.info("Starting to load data into postgres")
      start_load = time.time()
      
      load_into_postgres(cfg)
       
      end_load = time.time() - start_load
      logger.info("Finished load data into postgres | dur = %.4f", end_load)
   except Exception:
      logger.exception("Exception occured while load data on pipeline")
      raise
   
   end = time.time() - start
   
   logger.info("PIPELINE END | dur = %.4fs", end)
   logger.info("=" * 100) 
   logger.info("")
   
   
if __name__ == "__main__":
   main()

"""
====================== ANALISIS ======================

Status:
- File ini masih kosong, padahal README mengarahkan user untuk menjalankan
  `python src/pipeline.py`. Ini adalah gap paling besar di struktur project.

Yang kurang:
1. Belum ada orchestration untuk urutan `extract -> transform -> load ->
   validate`.
2. Belum ada fail-fast control flow, sehingga status sukses/gagal pipeline
   tidak terpusat.
3. Belum ada pengukuran durasi total dan durasi per stage.
4. Belum ada `run_id`, summary output, exit code, dan error boundary yang
   jelas untuk integrasi dengan scheduler.
5. Belum ada opsi menjalankan stage tertentu saja, padahal itu penting untuk
   rerun parsial saat debugging atau recovery.

Agar lebih profesional:
- Jadikan file ini sebagai single entrypoint pipeline.
- Pusatkan bootstrap config, logger, dan lifecycle eksekusi di sini.
- Tambahkan orchestration yang eksplisit, dependency check, serta step
  summary di akhir run.
- Siapkan struktur agar mudah dipasang ke Airflow, Prefect, Cron, atau CI/CD.

==========================================================================
"""

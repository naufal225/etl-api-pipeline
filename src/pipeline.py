from extract.extract_api import extract_all
from transform.transform_join import transform
from load.load_postgres import load_into_postgres
from logger import build_logger
from config import PgConfig
from dotenv import load_dotenv
from pathlib import Path
import time
import uuid

BASE_DIR = Path(__file__).resolve().parents[1]

LOG_PATH = BASE_DIR / "logs" / "etl-run.log"
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

run_id = str(uuid.uuid4())

logger = build_logger(LOG_PATH, run_id=run_id)

summary = {}

def run_stage(name, fun, *args, **kwargs):
   logger.info("[START] %s", name)
   start = time.perf_counter()
   
   try:
      result = fun(*args, **kwargs)
      duration = time.perf_counter() - start
      
      summary[name] = "succeed"
      
      logger.info("[FINISHED] %s | dur = %.4fs", name, duration)
      return result
   except Exception:
      duration = time.perf_counter() - start
      
      summary[name] = "failed"
      
      logger.exception("[FAILED] %s | dur = %.4fs", name, duration)
      raise

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
   
   try:
      start = time.time()
   
      cfg = load_config()

      raw_path = run_stage("extract", extract_all)  

      processed_path = run_stage("transform", transform, raw_path)   

      run_stage("load", load_into_postgres, processed_path, cfg)

   finally:
      end = time.time() - start

      logger.info("PIPELINE END | dur = %.4fs", end)
      logger.info("Summary: ")

      for k, v in summary.items():
         logger.info("%s\t: %s", k, v)

      logger.info("=" * 100) 
      logger.info("")
   
   
if __name__ == "__main__":
   main()

"""
====================== ANALISIS ======================

Status:
- File ini sudah berfungsi sebagai entrypoint dasar untuk `extract ->
  transform -> load`, jadi fondasi orchestration sudah ada.
- Dibanding file lain, `pipeline.py` sekarang sudah menuju arah yang benar,
  tetapi masih perlu refinement agar layak dipakai sebagai pipeline runner
  yang rapi, observability-friendly, dan mudah dipasang ke scheduler.

Yang sudah baik:
1. Urutan stage sudah eksplisit dan mudah dibaca.
2. Setiap stage punya logging start/end dan durasi dasar.
3. Pipeline memakai fail-fast behavior: jika satu tahap gagal, proses berhenti.
4. Config dimuat sekali di entrypoint sebelum load ke PostgreSQL.

Gap utama:
1. Pola `try/except` per stage masih duplikatif. Ini membuat pipeline runner
   cepat membesar saat nanti ditambah stage `validate`, `publish`, atau
   notifikasi.
2. `load_config()` di file ini menduplikasi concern yang juga ada di
   `load_postgres.py`. Config bootstrap sebaiknya hanya ada satu sumber.
3. Belum ada stage `validate_db`, sehingga pipeline berhenti di load tanpa
   post-load data quality verification.
4. Jika stage gagal, log `PIPELINE END` tidak dijamin tercetak karena belum ada
   blok `finally` untuk summary akhir.
5. Belum ada `run_id`, status summary per stage, exit code yang jelas, dan
   output terstruktur yang cocok untuk Cron, Airflow, Prefect, atau CI job.
6. Belum ada guard untuk artifact dependency, misalnya memastikan file hasil
   extract/transform benar-benar ada sebelum melanjutkan ke tahap berikutnya.
7. Pengukuran durasi memakai `time.time()`. Untuk runtime measurement, lebih
   stabil memakai `time.perf_counter()`.
8. Styling kode masih kurang rapi: indentasi di `main()` tidak konsisten dan
   ada blank log `logger.info("")` yang tidak terlalu memberi nilai operasional.

Saran refinement:
1. Buat helper seperti `run_stage(stage_name, fn, *args, **kwargs)` agar
   logging, timing, exception handling, dan status collection tidak duplikatif.
2. Tambahkan `validate_db()` setelah load agar pipeline punya quality gate
   akhir sebelum dinyatakan sukses.
3. Simpan hasil eksekusi ke summary object sederhana, misalnya:
   `{"extract": "success", "transform": "success", "load": "success"}`.
4. Bungkus lifecycle pipeline dalam `try/except/finally` level atas agar log
   akhir selalu tercetak, baik sukses maupun gagal.
5. Pusatkan `load_dotenv()` dan `PgConfig.from_env()` hanya di entrypoint ini,
   lalu module lain cukup menerima object config yang sudah jadi.
6. Tambahkan `run_id` dan sertakan di semua log agar satu eksekusi pipeline
   bisa ditelusuri end-to-end.
7. Siapkan opsi future-proof untuk selective run, misalnya hanya menjalankan
   `extract + transform` saat debugging lokal.

Refinement target yang lebih ideal:
- bootstrap logger/config
- generate run_id
- run extract
- validate artifact raw
- run transform
- validate artifact processed
- run load
- run validate_db
- print final summary + exit code

Kesimpulan:
- `pipeline.py` sudah bukan sekadar placeholder; ini sudah jadi entrypoint
  dasar yang valid.
- Refinement terpenting berikutnya adalah mengurangi duplikasi orchestration,
  menambah post-load validation, dan membuat execution summary yang kuat untuk
  kebutuhan operasional.

==========================================================================
"""

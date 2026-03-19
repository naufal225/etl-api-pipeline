from pathlib import Path
import logging

class RunIdFilter(logging.Filter):
   def __init__(self, run_id):
       self.run_id = run_id
       
   def filter(self, record):
      record.run_id = self.run_id
      return True

def build_logger(log_path: Path, run_id: str) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger = logging.getLogger()
    
    print(logger)
    
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = True
    
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s | run_id=%(run_id)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # File handler
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    
   #  tambahkan filter
   
    run_filter = RunIdFilter(run_id=run_id)
    fh.addFilter(run_filter)
    ch.addFilter(run_filter)
    
    logger.addHandler(fh)
    logger.addHandler(ch)
    
    return logger

"""
====================== ANALISIS ======================

Status:
- Logging dasar sudah berfungsi, tetapi observability masih terlalu minimal
  untuk ETL pipeline yang dijalankan rutin atau dijadwalkan.

Gap utama:
1. Semua module memakai nama logger yang sama: `ETL_Pipeline`. Ini membuat
   asal log per file atau per tahap sulit dilacak.
2. `logger.handlers.clear()` berisiko menghapus handler yang sudah dipasang
   oleh module lain ketika file ini diinisialisasi ulang.
3. Belum ada `RotatingFileHandler` atau strategi rotasi log. File log akan
   terus membesar saat pipeline dijalankan berkali-kali.
4. Format log belum memuat konteks penting seperti `run_id`, nama tahap,
   module, atau jumlah row yang diproses.
5. Belum ada kontrol level log dari environment, sehingga sulit membedakan
   mode dev, debug, dan production.

Agar lebih profesional:
- Gunakan logger per module (`__name__`) atau child logger per stage.
- Tambahkan run identifier untuk setiap eksekusi pipeline.
- Gunakan rotating log handler atau structured logging JSON.
- Pisahkan konfigurasi logging dari business logic.

==========================================================================
"""

import os
from dataclasses import dataclass

def _required_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RecursionError(f"Missing required environtment key: {key}")
    return val

def _env(key: str, default: str) -> str:
    val = os.getenv(key=key, default=default)
    return val

def mark_secret(pwd: str, keep: int = 3) -> str :
    if pwd is None:
        return "<none>"
    if len(pwd) <= keep:
        return "*" * keep
    return pwd[:keep] + (len(pwd) - keep) * "*"
    
@dataclass(frozen=True)
class PgConfig:
    host: str
    port: str
    dbname: str
    user: str
    password: str
    
    @staticmethod
    def from_env() -> "PgConfig":
        host = _env("PGHOST", "localhost")
        port = _env("PGPORT", 5432)
        dbname = _required_env("PGDATABASE")
        user = _env("PGUSER", "postgres")
        password = _required_env("PGPASSWORD")
        
        return PgConfig(host, port, dbname, user, password)
    
    def safe_str(self) -> str:
        return (
            f"PgConfig(host={self.host}, port={self.port}, dbname={self.dbname}, "
            f"user={self.user}, password={mark_secret(self.password)})"
        )

"""
====================== ANALISIS  ======================

Status:
- Fondasi konfigurasi sudah ada, tetapi belum cukup aman dan eksplisit untuk
  pipeline ETL yang ingin naik ke level produksi.

Gap utama:
1. `_required_env()` melempar `RecursionError`, padahal masalahnya adalah
   konfigurasi. Ini membuat troubleshooting menyesatkan. Gunakan `ValueError`
   atau custom `ConfigError`.
2. `safe_str()` masih mencetak password utuh ke log. Secret wajib di-redact.
3. `port` didefinisikan sebagai `str`, tetapi default diberikan sebagai angka.
   Untuk standar industri, config harus dicast dan divalidasi secara eksplisit.
4. Validasi environment masih minim. Belum ada `sslmode`, `connect_timeout`,
   `application_name`, atau namespace config per environment.
5. Bootstrap config belum dipusatkan. Saat ini `load_dotenv()` dipanggil dari
   layer load, bukan dari layer konfigurasi atau entrypoint pipeline.

Agar lebih profesional:
- Buat object config yang tervalidasi penuh sejak awal pipeline.
- Redact semua credential sebelum logging.
- Tambahkan custom exception untuk config.
- Sediakan builder DSN / connection params yang reusable lintas module.

==========================================================================
"""

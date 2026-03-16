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
            f"user={self.user}, password={self.password})"
        )
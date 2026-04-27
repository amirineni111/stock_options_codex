from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseModel
import os


class AppSettings(BaseModel):
    polygon_api_key: Optional[str] = None
    db_path: Path = Path("data/options_screening.sqlite3")
    request_timeout_seconds: float = 20.0


def _default_db_path() -> Path:
    local_app_data = os.getenv("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / "StockOptionsCodex" / "options_screening.sqlite3"
    return Path("data/options_screening.sqlite3")


def _resolve_db_path(value: str = None) -> Path:
    raw_path = value or str(_default_db_path())
    return Path(os.path.expanduser(os.path.expandvars(raw_path)))


def get_settings() -> AppSettings:
    load_dotenv()
    return AppSettings(
        polygon_api_key=os.getenv("POLYGON_API_KEY"),
        db_path=_resolve_db_path(os.getenv("OPTIONS_DB_PATH")),
    )

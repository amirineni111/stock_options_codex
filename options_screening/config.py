from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseModel
import os


class AppSettings(BaseModel):
    polygon_api_key: Optional[str] = None
    db_path: Path = Path("data/options_screening.sqlite3")
    request_timeout_seconds: float = 20.0


def get_settings() -> AppSettings:
    load_dotenv()
    return AppSettings(
        polygon_api_key=os.getenv("POLYGON_API_KEY"),
        db_path=Path(os.getenv("OPTIONS_DB_PATH", "data/options_screening.sqlite3")),
    )

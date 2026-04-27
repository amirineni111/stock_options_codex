from datetime import date, timedelta
from typing import List

from pydantic import BaseModel

from .config import AppSettings
from .models import RejectedContract, ScoredContract
from .polygon import PolygonClient
from .scoring import score_contracts
from .storage import Storage


class ScanRequest(BaseModel):
    tickers: List[str]
    fixed_risk: float = 250.0
    min_volume: int = 50
    min_open_interest: int = 250
    max_spread_pct: float = 12.0
    min_days_to_expiration: int = 21
    max_days_to_expiration: int = 75
    min_abs_delta: float = 0.25
    max_abs_delta: float = 0.65
    min_iv: float = 0.05
    max_iv: float = 1.2
    max_contracts_per_ticker: int = 50
    allow_missing_spread: bool = False


class ScanSummary(BaseModel):
    accepted: int = 0
    rejected: int = 0
    errors: int = 0


def run_scan(settings: AppSettings, storage: Storage, request: ScanRequest) -> ScanSummary:
    client = PolygonClient(settings.polygon_api_key, settings.request_timeout_seconds)
    storage.start_scan(request.model_dump())

    summary = ScanSummary()
    today = date.today()
    expiration_gte = today + timedelta(days=request.min_days_to_expiration)
    expiration_lte = today + timedelta(days=request.max_days_to_expiration)
    all_accepted: List[ScoredContract] = []
    all_rejected: List[RejectedContract] = []

    for ticker in request.tickers:
        try:
            contracts = client.get_option_chain_snapshots(
                ticker,
                expiration_gte=expiration_gte,
                expiration_lte=expiration_lte,
                limit=request.max_contracts_per_ticker,
            )
            accepted, rejected = score_contracts(contracts, request)
            all_accepted.extend(accepted)
            all_rejected.extend(rejected)
            summary.accepted += len(accepted)
            summary.rejected += len(rejected)
            storage.log_ticker(ticker, len(accepted), len(rejected), None)
        except Exception as exc:  # Dashboard should keep scanning other symbols.
            summary.errors += 1
            storage.log_ticker(ticker, 0, 0, _sanitize_error(str(exc), settings.polygon_api_key))

    all_accepted.sort(key=lambda item: item.score, reverse=True)
    storage.save_results(all_accepted)
    storage.save_rejections(all_rejected)
    storage.finish_scan(summary.model_dump())
    return summary


def _sanitize_error(message: str, api_key: str = None) -> str:
    if not message:
        return message
    safe = message
    if api_key:
        safe = safe.replace(api_key, "REDACTED")
    return safe

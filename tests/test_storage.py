from datetime import date, timedelta

from options_screening.models import OptionContract
from options_screening.scanner import ScanRequest
from options_screening.scoring import score_contract
from options_screening.storage import Storage


def test_storage_round_trip(tmp_path):
    storage = Storage(tmp_path / "screen.sqlite3")
    storage.initialize()
    storage.start_scan(ScanRequest(tickers=["AAPL"]).model_dump())
    contract = OptionContract(
        underlying="AAPL",
        contract_ticker="O:AAPL260619C00200000",
        contract_type="call",
        expiration_date=date.today() + timedelta(days=45),
        strike_price=200.0,
        bid=2.4,
        ask=2.6,
        last_price=2.5,
        open_interest=1000,
        volume=200,
        implied_volatility=0.45,
        delta=0.42,
    )
    scored, _ = score_contract(contract, ScanRequest(tickers=["AAPL"], fixed_risk=500))
    storage.save_results([scored])
    storage.finish_scan({"accepted": 1, "rejected": 0, "errors": 0})

    frame = storage.load_latest_results()

    assert len(frame) == 1
    assert frame.iloc[0]["underlying"] == "AAPL"

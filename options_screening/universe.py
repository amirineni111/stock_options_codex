from pathlib import Path
from typing import List, Tuple

import pandas as pd


FALLBACK_LIQUID_TICKERS = [
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "META",
    "GOOGL",
    "GOOG",
    "AVGO",
    "TSLA",
    "BRK.B",
    "JPM",
    "LLY",
    "V",
    "UNH",
    "XOM",
    "MA",
    "COST",
    "HD",
    "PG",
    "NFLX",
    "WMT",
    "BAC",
    "ABBV",
    "CRM",
    "AMD",
    "KO",
    "PEP",
    "MRK",
    "ORCL",
    "CVX",
    "WFC",
    "CSCO",
    "MCD",
    "DIS",
    "ABT",
    "INTU",
    "QCOM",
    "IBM",
    "GE",
    "CAT",
    "NOW",
    "TXN",
    "AMAT",
    "UBER",
    "SPY",
    "QQQ",
]


def load_sp500_tickers() -> Tuple[List[str], str]:
    data_path = Path("data/sp500_tickers.csv")
    if data_path.exists():
        frame = pd.read_csv(data_path)
        if "symbol" in frame.columns and not frame.empty:
            return frame["symbol"].dropna().astype(str).str.upper().tolist(), ""

    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        frame = tables[0]
        tickers = frame["Symbol"].astype(str).str.replace(".", "/", regex=False).str.upper().tolist()
        data_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"symbol": tickers}).to_csv(data_path, index=False)
        return tickers, ""
    except Exception:
        return FALLBACK_LIQUID_TICKERS, "Could not load the live S&P 500 list, so the dashboard is using a liquid starter universe."

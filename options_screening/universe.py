"""
Ticker universes, and the one place symbol-format differences are reconciled.

Share classes are written differently by every vendor: Polygon wants ``BRK.B``,
Yahoo wants ``BRK-B``, and Wikipedia publishes ``BRK.B``. The scraper used to rewrite
the dot to a *slash* (``BRK/B``), which no provider here accepts — so those symbols
silently errored out of every scan while the hard-coded lists in this same file used
the correct ``BRK.B``. Canonical form in this repo is the Polygon one; converting for
Yahoo is ``to_yahoo_symbol``'s job and happens at the client boundary.
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import pandas as pd

# Resolved against the package, not the process working directory. A relative
# "data/..." resolves differently depending on where streamlit was launched from,
# which meant the cache was sometimes written somewhere it would never be read.
_DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# Bumped when the stored symbol format changes, so a cache written by the version
# that produced "BRK/B" is ignored rather than silently reused forever.
_SP500_CACHE = _DATA_DIR / "sp500_tickers_v2.csv"


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

SP100_TICKERS = [
    "AAPL",
    "ABBV",
    "ABT",
    "ACN",
    "ADBE",
    "AIG",
    "AMD",
    "AMGN",
    "AMT",
    "AMZN",
    "AVGO",
    "AXP",
    "BA",
    "BAC",
    "BK",
    "BKNG",
    "BLK",
    "BMY",
    "BRK.B",
    "C",
    "CAT",
    "CHTR",
    "CL",
    "CMCSA",
    "COF",
    "COP",
    "COST",
    "CRM",
    "CSCO",
    "CVS",
    "CVX",
    "DE",
    "DHR",
    "DIS",
    "DOW",
    "DUK",
    "EMR",
    "EXC",
    "F",
    "FDX",
    "GD",
    "GE",
    "GILD",
    "GM",
    "GOOG",
    "GOOGL",
    "GS",
    "HD",
    "HON",
    "IBM",
    "INTC",
    "INTU",
    "JNJ",
    "JPM",
    "KHC",
    "KO",
    "LIN",
    "LLY",
    "LMT",
    "LOW",
    "MA",
    "MCD",
    "MDLZ",
    "MDT",
    "MET",
    "META",
    "MMM",
    "MO",
    "MRK",
    "MS",
    "MSFT",
    "NEE",
    "NFLX",
    "NKE",
    "NVDA",
    "ORCL",
    "PEP",
    "PFE",
    "PG",
    "PM",
    "PYPL",
    "QCOM",
    "RTX",
    "SBUX",
    "SCHW",
    "SO",
    "SPG",
    "T",
    "TGT",
    "TMO",
    "TMUS",
    "TSLA",
    "TXN",
    "UNH",
    "UNP",
    "UPS",
    "USB",
    "V",
    "VZ",
    "WBA",
    "WFC",
    "WMT",
    "XOM",
]


def normalize_symbol(symbol: str) -> str:
    """
    Canonical (Polygon) form: uppercase, dot-separated share class.

    Accepts the dash and slash variants so a stale cache file or a hand-typed
    ``BRK-B`` in the custom-ticker box still resolves.
    """
    return str(symbol).strip().upper().replace("/", ".").replace("-", ".")


def to_yahoo_symbol(symbol: str) -> str:
    """Yahoo's chart API wants ``BRK-B`` where Polygon wants ``BRK.B``."""
    return normalize_symbol(symbol).replace(".", "-")


def load_sp500_tickers() -> Tuple[List[str], str]:
    if _SP500_CACHE.exists():
        try:
            frame = pd.read_csv(_SP500_CACHE)
            if "symbol" in frame.columns and not frame.empty:
                cached = [normalize_symbol(s) for s in frame["symbol"].dropna().astype(str)]
                if cached:
                    return cached, ""
        except Exception:
            # A corrupt cache is not worth failing the app over — fall through and
            # refetch, which also rewrites the file.
            pass

    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        frame = tables[0]
        tickers = [normalize_symbol(s) for s in frame["Symbol"].astype(str)]
        _SP500_CACHE.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"symbol": tickers}).to_csv(_SP500_CACHE, index=False)
        return tickers, ""
    except Exception:
        return (
            FALLBACK_LIQUID_TICKERS,
            "Could not load the live S&P 500 list, so the dashboard is using a liquid starter universe.",
        )


def load_sp100_tickers() -> Tuple[List[str], str]:
    return SP100_TICKERS, ""

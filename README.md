# Local Options Screening Dashboard

A local, Python-first options screener for conservative swing-trade call and put ideas. It uses Polygon market data, ranks contracts with transparent score components, and stores scan history in SQLite.

This is a decision-support tool only. It does not place trades and does not connect to a broker.

## Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Edit `.env` and set:

```text
POLYGON_API_KEY=your_polygon_key
```

Real-time options data requires Polygon permissions that may need a paid plan.

## Run

```powershell
streamlit run app.py
```

The dashboard opens in your browser. It can scan a selected S&P 500 batch manually, and it can auto-refresh during regular US market hours.

The auto-refresh interval is configurable from the sidebar. Enable **Auto-refresh during market hours**, then choose seconds or minutes and set the interval, such as 15 minutes for delayed data or 1 minute for a more frequent scan cadence.

## Test

```powershell
pytest
```

## Workflow

1. Start the dashboard.
2. Confirm the Polygon key is loaded in the sidebar.
3. Choose call/put filters and fixed-dollar max risk.
4. Run a manual scan for a small batch first.
5. Increase the ticker limit or enable auto-refresh when the data plan and rate limits are comfortable.

## Safety Notes

- Results are screening signals, not guaranteed recommendations.
- Stale, incomplete, or illiquid contracts are rejected from rankings.
- Live trading, broker orders, and automated execution are intentionally out of scope for v1.

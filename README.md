# Options & Intraday Screening Dashboard

A local, Python-first screener with two lanes that share one engine:

- **Options Scanner** — ranks single-leg calls and puts on Polygon data, with an
  ATR-derived bracket expressed in *premium* terms and a decay model that finally uses
  the theta and vega the API has been returning all along.
- **Intraday Stocks** — a near-real-time equity screener on free Yahoo data, scoring
  each ticker with a multi-factor engine and showing BUY/SELL signals with
  entry/stop/target levels.

Every actionable signal in both lanes is automatically forward-tested against its stop
and target, so the Performance tab reports **measured** win rates and expectancy rather
than a backtest, and grades every call as SUCCESS or FAILED. New signals and their
outcomes can be pushed to your phone. This is a decision-support tool: it does not place
trades and does not connect to a broker.

> Not financial advice. Signals are heuristics for screening, not trade recommendations.

## Quick start

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
streamlit run app.py
```

Or run `start_options_dashboard.bat`. It opens the dashboard on port 8501, or on the next
free port if 8501 is taken, and prints the port it chose. `stop_options_dashboard.bat` stops it.

The intraday lane needs no API key. The options lane needs `POLYGON_API_KEY` (Polygon is
now [Massive](https://massive.com); the same key works). The **Options Starter** plan is
enough, with two things to know about it:

- It is **15-minute delayed** and has **no bid/ask quotes**. Tick *Allow missing bid-ask
  spread* (and *Ignore missing bid-ask for trade signal*) in the sidebar, or every
  contract is rejected. Forward tests are then charged an *estimated* spread from
  open-interest tiers rather than zero (`tracked_cost_pct` in `scanner.py`).
- It has no stock data beyond a 5-calls/minute free tier, so the underlying's price,
  trend and ATR come from Yahoo (the data yfinance wraps), in one batched request per
  scan. The earnings check needs the separate Benzinga entitlement and says so when it
  is missing.

## Configuration

Optional overrides go in `.env` (see `.env.example`):

- `POLYGON_API_KEY` — required for the options lane only.
- `OPTIONS_ALERT_WEBHOOK_URL` — where to push alerts (see below). Blank = log only.
- `OPTIONS_DB_PATH` — SQLite location. Defaults to
  `%LOCALAPPDATA%\StockOptionsCodex\options_screening.sqlite3`, deliberately outside
  synced folders: OneDrive and SQLite WAL files produce lock and sync conflicts.

The key is read from the environment only. It is never written to disk by the app, and
it is redacted from every URL and exception message (`options_screening/polygon.py`).

## Phone alerts

Two moments trigger a push:

- **A new signal** — when a scan arms a setup for forward testing. Several contracts on
  one underlying in the same scan are one notification, best-scored first, with the
  premium bracket *and* the underlying's price levels (the position is managed on the
  stock even though its P&L is in premium).
- **Its outcome** — when it resolves: **SUCCESS** (target first), **FAILED** (stop
  first) or **EXPIRED** (neither in time), with R and the lane's running record.

Setup for iPhone, with no account:

1. Install **ntfy** from the App Store and subscribe to a long, unguessable topic name
   (anyone who knows the name can read it).
2. Put `OPTIONS_ALERT_WEBHOOK_URL=https://ntfy.sh/<that-topic>` in `.env`, or paste it
   into *Phone alerts* in the dashboard sidebar.
3. Press *Send test alert*, or run `python scripts/run_alerts.py --test-push`.

The dashboard only alerts while a tab is open and its timer fires. For alerts with no
browser open, run **`start_options_alerts.bat`** — a headless runner that scans every
15 minutes (the cadence the delayed data moves at) with the dashboard's saved settings:

```
python scripts/run_alerts.py                           # options lane, 09:45-16:20 ET weekdays
python scripts/run_alerts.py --lanes options intraday  # both pages
python scripts/run_alerts.py --once                    # one cycle now, then exit
python scripts/run_alerts.py --sample-alert            # a made-up signal + outcome, real format
```

The runner and the dashboard can run together: arming is deduplicated and each alert is
claimed in the `alerts` table before it is pushed, so nothing is pushed twice. Every
alert is logged in the **Alerts** tab with its push result and the current state of the
trade, so a dead webhook shows up as failures rather than silence.

## The prediction scorecard

Every armed signal predicts *target before stop*, so it can be graded without
interpretation. The Performance tab shows success, failed and expired counts and
**accuracy = success / (success + failed)** — a trade that never reached either level is
not evidence either way. That deliberately differs from the net win rate, which counts a
timeout that drifted into profit as a win: a system can call direction well and still
lose money, or the reverse.

*Break down by* splits the scorecard by predicted signal, side (call/put, long/short),
score band, model probability, ticker, week and exit reason. **Score band** is the one
to watch: if accuracy does not rise with the score, the score is not measuring edge.

## Two directional reads, graded against each other

An option is a bet on direction plus a contract. The contract `score` (liquidity,
spread, delta, DTE, IV) measures only the contract; the **direction** comes from one of
two independent reads, and every contract carries both labels:

| Label | Direction from | Signals |
|---|---|---|
| `trade_signal` | The moving-average stack: price > SMA20 > SMA50 for calls, the reverse for puts | `BUY_CALL_CANDIDATE` / `BUY_PUT_CANDIDATE` |
| `engine_signal` | The intraday lane's multi-factor engine run on the underlying's **daily** bars: regime-weighted momentum and mean reversion, a 20-session breakout, weekly-trend and SMA confluence, daily support/resistance (`direction.py`) | `ENGINE_BUY_CALL` / `ENGINE_BUY_PUT` |

Both apply the same contract checks, so a difference in outcome is about direction.
Both are forward-tested and alerted (engine alerts are marked `[engine]`), and the
scorecard's **Direction source** breakdown says which is right more often. Neither
replaces the other until one has a measured record. Engine rows are graded but kept out
of model training until then.

Three things keep the grading honest:

- **One tracked bet per underlying and label.** Five AMD calls armed together are one
  bet on AMD; tracking all five would grade one stock move as five predictions. The
  best-scored contract is tracked; the rest still show in the ranked tables.
- **No trading on stale prints.** Without quotes, the entry is the last trade. A
  contract that has not traded in the last 60 minutes is WATCH_ONLY ("last trade ... ago
  - price may be stale"). Off-hours scans therefore track nothing, by design.
- **IV rank over a real history.** One IV reading per underlying per session, in its own
  table, ranked over the last year. It used to be read from the scan-results table,
  which keeps only 10 scans — about 2.5 hours at a 15-minute cadence.

## The learning loop

The rules **propose** a setup; a trained model **disposes**. The two never swap roles:
the model can only veto, never promote something the rules rejected.

1. **Log.** Every actionable signal is armed for forward-testing *with its feature
   vector attached*. Without this the outcome rows are unlearnable — the snapshot
   tables are replaced each scan, so by the time a trade resolves its inputs are gone.
2. **Resolve.** Later scans check open signals against fresh data — stocks against
   their 15-minute bars, options against **the contract's own 15-minute bars** since
   entry. A stop or target touch records a WIN/LOSS **net of estimated cost**, linked
   back to the tracking row. Within a single bar the stop is checked first, because
   there is no way to know which was touched and assuming the target would inflate the
   win rate exactly on the most volatile bars. An option that gaps through its stop is
   filled at the open, not at the stop.
3. **Train.** Once ~120 resolved trades accumulate, the Model tab (or
   `scripts/train_model.py`) walk-forward evaluates an L2 logistic regression and
   reports out-of-sample AUC, Brier, calibration, and expectancy at each threshold.
4. **Gate.** A candidate saves **inactive**. It passes only if it beats chance out of
   sample *and* turns a profit at its decision threshold. Promoting is a separate,
   deliberate click, and any earlier model can be rolled back.
5. **Shadow.** Before gating on it, run the candidate in **shadow**: it scores every
   directional setup and logs the probability but vetoes nothing. This is the only way
   to learn what it would do to the trades it wants to block — once it is gating, those
   trades stop happening and stop being measurable. Tracked rows record which mode
   produced their probability (`model_mode`), because gated rows are a censored sample
   and must never be pooled with shadow rows.
6. **Serve.** The active model scores each setup live. Below the cost-adjusted
   breakeven win rate plus a margin, an otherwise-actionable signal is downgraded to
   WATCH_ONLY with the reason shown.

```
python scripts/train_model.py --lane intraday              # evaluate + save, do not activate
python scripts/train_model.py --lane options --activate    # promote, if it clears the gate
python scripts/train_model.py --lane intraday --shadow      # run alongside, veto nothing
python scripts/model_report.py --lane intraday --mode shadow # judge on real resolved trades
```

Editing `FEATURE_NAMES` means bumping `FEATURE_VERSION` in
`options_screening/features.py`: the scanner refuses to serve a model built on a
different feature contract, so it fails closed to rules-only rather than serving
misaligned probabilities.

**Why logistic regression and not gradient boosting.** The effective sample is far
smaller than the row count — equities move together intraday, so simultaneous longs
across AAPL/MSFT/NVDA are largely one beta bet, and a chain's contracts on one
underlying are more correlated still. The decision rule also needs *calibration*, not
ranking: the gate compares a probability against a breakeven, so a model that ranks
perfectly but reports 0.9 where the truth is 0.5 is worse than useless. See the module
docstring in `options_screening/model.py`.

## How the options lane values a contract

Greeks come from Polygon; the projection is a bounded Taylor expansion:

```
dP  ~  delta*dS  +  0.5*gamma*dS^2  +  theta*dt  +  vega*dIV
```

The `theta*dt` term is what the original scenario columns omitted. Over a 21–75 day
hold it is usually the largest component of the P&L, and leaving it out made every
favourable scenario read better than it was. A worked example, using a PEP $155 call at
$5.45 with delta 0.536:

- Contract cost is `5.45 x 100 = $545`; expiration breakeven is `155 + 5.45 = 160.45`.
- A **$2 move today** is roughly `0.536 x 2 = $1.07` per share, so about `$652` — a
  $105 gain, *before* decay.
- **At expiration with PEP at $157**, intrinsic value is `157 - 155 = $2.00`, so the
  contract is worth `$200` against the `$545` paid — a **$345 loss**, despite the stock
  having moved in the right direction.

That gap between the two is time value, and it is why `theta_per_premium`,
`decay_at_target` and `target_hold_days` are columns rather than footnotes. The holding
period behind them scales with the **square** of distance in ATRs, because a random walk
covers `N x ATR` in about `N²` days — dividing distance by ATR understates the hold, and
understating the hold understates decay.

`premium_rr` is reported separately from the underlying's fixed 1.5 reward:risk, because
delta, gamma and decay all bend it — on a low-delta contract, a long way.

## Project layout

```
app.py                    Streamlit UI (Options Scanner + Intraday Stocks pages)
options_screening/
  polygon.py              Polygon REST client (cursor-safe pagination, retry/backoff)
  yahoo_client.py         Batched Yahoo client (forming-candle drop, TTL caches)
  indicators.py           RSI/EMA/MACD/ATR/ADX/Bollinger/VWAP/S&R (pure-Python OHLC math)
  signals.py              Intraday scoring engine, regime gate, cost model, trade levels
  scoring.py              Option contract filtering, scoring, decision context
  greeks.py               Premium projection, IV rank, the premium bracket
  relative_strength.py    Relative strength vs SPY
  scanner.py              Options scan orchestration + arming + resolution
  intraday.py             Intraday scan orchestration (two-phase: parallel, then rescore)
  features.py             Canonical feature contracts - where serving and training agree
  model.py                Logistic model, walk-forward evaluation, calibration metrics
  training.py             Retrain pipeline + quality gate, shared by the CLI and the UI
  storage.py              SQLite persistence, forward-test resolution, model store
  ui_panels.py            Alerts, Performance and Model tabs, shared by both lanes
  alerts.py               Push alerts (ntfy / Discord / Slack / webhook) for signals and outcomes
  scorecard.py            SUCCESS / FAILED / EXPIRED grading and its breakdowns
  direction.py            The daily multi-factor read behind the second options label
  runner.py               The headless runner's schedule and settings mapping
  market_hours.py         US equity market phases (America/New_York)
  timeutil.py             One timestamp parser + the intraday session clock
  models.py, config.py, universe.py, refresh.py
scripts/
  train_model.py          Retrain from the CLI, with the same gate as the dashboard
  model_report.py         Judge a shadow/active model on live resolved trades
  run_alerts.py           Headless 15-minute scanner that pushes alerts
tests/                    pytest suite (317 tests, all offline)
```

## Tests

```powershell
.venv\Scripts\python -m pytest tests\ -q
```

No network, no mocking library — fakes are hand-written and clocks are injected, so the
whole suite runs in seconds.

## Suggested settings for a wider scan

The defaults are conservative. To surface more candidates while you get a feel for it:

| Setting | Wider value |
|---|---|
| Fixed dollar max risk | 750 |
| Minimum volume / open interest | 0 / 0 |
| Maximum bid-ask spread % | 50 |
| Days to expiration | 7 to 120 |
| Absolute delta range | 0.05 to 0.95 |
| Implied volatility range | 0.01 to 3.00 |
| Max contracts per ticker | 150 |

Note that loosening the *filters* does not loosen the **signal-stage floors**: a
contract that clears a relaxed filter can still be downgraded to WATCH_ONLY for thin
liquidity or a wide spread, deliberately, so a wider search cannot quietly promote an
unfillable contract to a candidate.

## Known limitations

- **No market-holiday calendar.** On a holiday the phase reads REGULAR while data
  simply stays stale; the "as of" caption reveals it.
- **Option data is 15-minute delayed and unquoted** on the Starter plan. Entries are
  the last traded price, not a mid, and cost is estimated. Option bars are trade-based,
  so an illiquid contract can sit untouched in the bars while its quote moves; such a
  trade resolves late rather than wrongly. When bars cannot be fetched the resolver
  falls back to the scan's snapshot price, which misses levels touched between scans.
- **IV rank is blank for a new install's first 10 sessions**, until there are enough
  daily readings to rank against. It is not backfilled: the Starter plan has no
  historical IV.
- **The per-ticker contract cap keeps the contracts nearest the money**, from strikes
  within 25% of the underlying. Raise *Max contracts per ticker* to reach further-out
  strikes and expiries.
- **Yahoo has no bid/ask**, so intraday transaction cost is *estimated* from liquidity
  tiers unless a Polygon quote is available; Yahoo intraday data can lag 1–2 minutes.
- **Forward-tested fills are optimistic in one direction:** a bar that trades through
  the target is credited at the target, with no slippage beyond the modelled round-trip
  cost.
- **Trades armed before feature logging existed are not trainable.** Their inputs are
  unrecoverable, and they are excluded rather than imputed.
- **The premium projection is local.** Moves are clamped to the range where delta stays
  inside [0,1] / [-1,0]; beyond that the projection flattens rather than remaining
  meaningful.
- Keep intraday watchlists under ~50 tickers to stay friendly with Yahoo rate limits.
- Single-leg only. No verticals, calendars or multi-leg construction.

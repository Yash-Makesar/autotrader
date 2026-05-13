# Qwen3 Gold Paper Trading Bot

A paper-trading bot for XAU/USD (Gold) that uses **Qwen3:8b** (via Ollama) for AI-powered trade signals, pure price action analysis, and a live Flask dashboard.

No real money is ever at risk — all trades are simulated.

## Features

- **AI signals** — Qwen3:8b analyses candle structure (body, wicks, swing levels) and outputs BUY / SELL / WAIT
- **Two modes** — `scalp` (1M candles, 5–10 pip SL/TP) and `swing` (15M candles, wider levels)
- **Live price feed** — Twelve Data WebSocket for real-time XAU/USD ticks; falls back to REST / gold-api.com
- **News filter** — ForexFactory USD high-impact event calendar blocks trading 30 min before / 15 min after events
- **Paper P&L** — Tracks balance, wins/losses, and win rate in both USD and INR
- **Telegram alerts** — Optional trade open/close notifications
- **Web dashboard** — Candlestick chart, live price, trade history, and bot status at `http://localhost:5001`

## Requirements

| Dependency | Notes |
|---|---|
| Python 3.11+ | 3.13 recommended |
| [Ollama](https://ollama.com) | Runs Qwen3 locally |
| `qwen3:8b` model | ~5 GB download |
| Twelve Data API key | Free tier: 800 req/day — [get one here](https://twelvedata.com) |

## Quick Start

```bash
# 1. Clone / download the project, then:
bash setup.sh

# 2. Fill in your API key
nano .env         # set TWELVE_DATA_API_KEY

# 3. Run bot + dashboard together
python start.py
```

Dashboard opens at **http://localhost:5001**

## Environment Variables

Copy `.env.example` to `.env` and fill in the values:

| Variable | Required | Description |
|---|---|---|
| `TWELVE_DATA_API_KEY` | **Yes** | Market data (candles + price) |
| `TELEGRAM_BOT_TOKEN` | No | Trade alerts via Telegram |
| `TELEGRAM_CHAT_ID` | No | Your Telegram chat ID |
| `GEMINI_API_KEY` | No | Unused by default |
| `NEWS_API_KEY` | No | Unused by default |

## Project Structure

```
tra/
├── gemini_paper_bot.py   # Bot engine (trading logic, Qwen3, news filter)
├── dashboard.py          # Flask web dashboard (port 5001)
├── start.py              # Launches bot + dashboard together
├── templates/
│   └── index.html        # Dashboard UI
├── paper_trades.csv      # Trade log (auto-created)
├── daily_summary.csv     # End-of-day summary (auto-created)
├── bot_state.json        # Live bot state read by the dashboard
├── requirements.txt
├── .env                  # Your API keys (never commit this)
└── setup.sh
```

## Running Individually

```bash
# Bot only (terminal output)
python gemini_paper_bot.py

# Dashboard only (requires bot_state.json to exist)
python dashboard.py
```

## Changing Mode

Edit `gemini_paper_bot.py`, line 64:

```python
MODE = "scalp"   # or "swing"
```

- **scalp** — runs every 1 minute, 1M candles, tight 5/6/10 pip SL/TP
- **swing** — runs every 15 minutes, 15M candles, fixed Setup A levels

## Configuration

Key constants at the top of `gemini_paper_bot.py`:

| Constant | Default | Description |
|---|---|---|
| `PAPER_BALANCE` | `50.0` USD | Starting paper balance |
| `RISK_PERCENT` | `0.005` | 0.5% risk per trade |
| `USD_TO_INR` | `95.5` | Exchange rate for INR display |
| `OLLAMA_MODEL` | `qwen3:8b` | Ollama model to use |

## Stopping the Bot

Press `Ctrl+C` — a final daily summary is printed before exit.

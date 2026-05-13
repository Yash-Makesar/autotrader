"""
Gold Bot Dashboard — Flask server
Run: python dashboard.py
Open: http://localhost:5000

Reads paper_trades.csv for trade history.
Fetches live candles + price from Twelve Data (same key as the bot).
"""

import os, json, time
import pandas as pd
from flask import Flask, jsonify, render_template, Response, stream_with_context
from datetime import datetime, timezone

from gemini_paper_bot import (
    fetch_candles, compute_indicators, fetch_gold_price,
    fetch_news, CFG, MODE, USD_TO_INR, PAPER_BALANCE,
)

app = Flask(__name__)
LOG_FILE   = "paper_trades.csv"
STATE_FILE = "bot_state.json"


# ── API endpoints ────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/candles")
def api_candles():
    try:
        tf = 1 if MODE == "scalp" else 15
        df = fetch_candles(n=120, tf_minutes=tf)
        df = compute_indicators(df)
        cols = ["time", "open", "high", "low", "close", "volume", "rsi", "atr"]
        records = df[cols].fillna(0).to_dict(orient="records")
        return jsonify({"ok": True, "mode": MODE, "tf": CFG["candle_tf"], "data": records})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/price")
def api_price():
    try:
        return jsonify({"ok": True, "price": fetch_gold_price()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/trades")
def api_trades():
    try:
        if not os.path.isfile(LOG_FILE):
            return jsonify({"ok": True, "data": [], "stats": _empty_stats()})
        df = pd.read_csv(LOG_FILE)
        return jsonify({"ok": True, "data": df.fillna("").to_dict(orient="records"),
                        "stats": _compute_stats(df)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/stream/price")
def stream_price():
    """SSE endpoint — pushes XAU/USD price whenever it changes (≈ every tick)."""
    def generate():
        last_price = None
        while True:
            try:
                price = fetch_gold_price()
                rounded = round(price, 2) if price else None
                if rounded is not None and rounded != last_price:
                    last_price = rounded
                    yield f"data: {json.dumps({'price': rounded})}\n\n"
            except GeneratorExit:
                break
            except Exception:
                pass
            time.sleep(1)
    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/status")
def api_status():
    try:
        if not os.path.isfile(STATE_FILE):
            return jsonify({"ok": True, "running": False, "open_trade": None,
                            "last_signal": None, "last_cycle_utc": None,
                            "usd_to_inr": USD_TO_INR})
        with open(STATE_FILE) as f:
            state = json.load(f)
        return jsonify({"ok": True, "running": True, **state})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/news")
def api_news():
    try:
        news = fetch_news()
        fetched = news.get("fetched_at")
        if hasattr(fetched, "isoformat"):
            news = {**news, "fetched_at": fetched.isoformat()}
        return jsonify({"ok": True, **news})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ── helpers ──────────────────────────────────────────────────────────────────

def _empty_stats():
    return {
        "balance_usd": PAPER_BALANCE,
        "balance_inr": round(PAPER_BALANCE * USD_TO_INR, 0),
        "pnl_usd": 0.0, "pnl_inr": 0.0, "pnl_pct": 0.0,
        "wins": 0, "losses": 0, "win_rate": 0.0, "total_trades": 0,
    }


def _compute_stats(df):
    if df.empty:
        return _empty_stats()
    pnl   = float(df["pnl_usd"].sum())
    wins  = int((df["pnl_usd"] > 0).sum())
    losses = int((df["pnl_usd"] <= 0).sum())
    total = len(df)
    bal   = PAPER_BALANCE + pnl
    return {
        "balance_usd":   round(bal, 2),
        "balance_inr":   round(bal * USD_TO_INR, 0),
        "pnl_usd":       round(pnl, 2),
        "pnl_inr":       round(pnl * USD_TO_INR, 0),
        "pnl_pct":       round(pnl / PAPER_BALANCE * 100, 2),
        "wins":          wins,
        "losses":        losses,
        "win_rate":      round(wins / total * 100 if total else 0, 1),
        "total_trades":  total,
    }


# ── entry ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("  Gold Bot Dashboard → http://localhost:5001")
    print(f"  Mode: {MODE.upper()} | TF: {CFG['candle_tf']}")
    print("=" * 50)
    app.run(debug=False, port=5001, host="0.0.0.0")

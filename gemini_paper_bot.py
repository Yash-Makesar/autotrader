"""
Qwen3 Gold Scalping Bot — Paper Trade Mode
===========================================
Features:
  - Paper trading (no broker needed, tracks P&L in INR)
  - ForexFactory economic calendar (USD high-impact event filter)
  - Pure price action analysis (candle structure, swing levels)
  - Scalping mode (1M/5M candles, tight SL/TP)
  - Swing mode (15M/1H candles, Setup A levels)
  - Full trade log with Qwen3 reasoning
  - Daily P&L summary

Requirements:
  pip install ollama pandas requests schedule

Local model used:
  - qwen3:8b via Ollama  → ollama.com  (free, runs locally)
    Install: ollama pull qwen3:8b

Free APIs used:
  - Gold candles  → Twelve Data (twelvedata.com, free 800 req/day, symbol: XAU/USD)
  - Gold price    → Twelve Data /price endpoint (fallback: gold-api.com)
  - News/Calendar → ForexFactory via nfs.faireconomy.media (free, no key)

Environment variables:
  TWELVE_DATA_API_KEY → required  (get free key at twelvedata.com)
  TELEGRAM_BOT_TOKEN  → optional
  TELEGRAM_CHAT_ID    → optional
"""

import os, json, time, csv, schedule, requests, random, re
import asyncio, threading
import pandas as pd
import ollama
import websockets
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()


# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════

OLLAMA_MODEL        = "qwen3:8b"
TWELVE_DATA_KEY     = os.getenv("TWELVE_DATA_API_KEY", "")
TELEGRAM_TOKEN      = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID")

USD_TO_INR       = 95.5          # update manually or fetch live
PAPER_BALANCE    = 50.0          # starting paper balance in USD (~₹4,775)
RISK_PERCENT     = 0.005         # 0.5% risk per trade — small trades
LOG_FILE         = "paper_trades.csv"
SUMMARY_FILE     = "daily_summary.csv"
STATE_FILE       = "bot_state.json"

_last_signal: dict | None = None   # persists last Qwen3 signal across cycles

# ── MODE SELECTOR ─────────────────────────────────────────────────────────────
# "scalp"  → 1M candles, 5–15 pip SL, runs every 1 minute
# "swing"  → 15M candles, Setup A levels, runs every 15 minutes
MODE             = "scalp"

SCALP_CONFIG = {
    "interval_minutes": 1,
    "candle_tf":        "M1",
    "sl_pips":          5,        # $5 SL — tight stop
    "tp1_pips":         6,        # $6 TP1 — take small profit fast
    "tp2_pips":         10,       # $10 TP2 — runner target
    "min_rsi":          35,
    "max_rsi":          70,
}

SWING_CONFIG = {
    "interval_minutes": 15,
    "candle_tf":        "M15",
    "entry_low":        4696.0,
    "entry_high":       4710.0,
    "sl":               4670.0,
    "tp1":              4742.0,
    "tp2":              4807.0,
    "min_rsi":          40,
    "max_rsi":          65,
}

CFG = SCALP_CONFIG if MODE == "scalp" else SWING_CONFIG


# ══════════════════════════════════════════════════════════════════════════════
# TWELVE DATA WEBSOCKET — real-time price feed
# ══════════════════════════════════════════════════════════════════════════════

class TwelveDataWebSocket:
    """
    Maintains a persistent WebSocket connection to Twelve Data and caches
    the latest XAU/USD price.  Runs in a daemon thread so it dies with the
    main process.  Falls back gracefully if the API key is missing.
    """
    WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"

    def __init__(self, api_key: str, symbol: str = "XAU/USD"):
        self._api_key = api_key
        self._symbol  = symbol
        self._price: float | None = None
        self._lock    = threading.Lock()
        self._started = False

    def start(self):
        if not self._api_key or self._started:
            return
        self._started = True
        t = threading.Thread(target=self._run, daemon=True, name="td-ws")
        t.start()

    def _run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._listen())

    async def _listen(self):
        url = f"{self.WS_URL}?apikey={self._api_key}"
        while True:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    await ws.send(json.dumps({
                        "action": "subscribe",
                        "params": {"symbols": self._symbol},
                    }))
                    async for raw in ws:
                        msg = json.loads(raw)
                        if msg.get("event") == "price" and "price" in msg:
                            with self._lock:
                                self._price = float(msg["price"])
            except Exception:
                await asyncio.sleep(5)   # wait before reconnecting

    def get_price(self) -> float | None:
        with self._lock:
            return self._price


_price_ws = TwelveDataWebSocket(TWELVE_DATA_KEY)
_price_ws.start()


# ══════════════════════════════════════════════════════════════════════════════
# PAPER TRADING ENGINE
# ══════════════════════════════════════════════════════════════════════════════

class PaperTrader:
    def __init__(self, balance_usd=PAPER_BALANCE):
        self.balance     = balance_usd
        self.start_bal   = balance_usd
        self.trades      = []
        self.open_trade  = None
        self.wins        = 0
        self.losses      = 0
        self.total_pnl   = 0.0

    def open(self, signal, current_price):
        """Simulate opening a trade."""
        risk_usd  = self.balance * RISK_PERCENT
        sl_dist   = abs(signal["entry"] - signal["sl"])
        units     = max(0.01, round(risk_usd / sl_dist, 2))

        self.open_trade = {
            "id":        len(self.trades) + 1,
            "direction": signal["signal"],
            "entry":     signal["entry"],
            "sl":        signal["sl"],
            "tp1":       signal["tp1"],
            "tp2":       signal["tp2"],
            "units":     units,
            "open_time": datetime.now(timezone.utc).isoformat(),
            "pattern":   signal.get("pattern", ""),
            "reason":    signal.get("reason", ""),
            "confidence":signal.get("confidence", ""),
            "news_clear":signal.get("news_clear", True),
        }
        print(f"  [PAPER] OPEN {signal['signal']} @ ${signal['entry']:.2f} | "
              f"SL ${signal['sl']:.2f} | TP1 ${signal['tp1']:.2f} | "
              f"Units: {units} | Risk: ${risk_usd:.2f} (~₹{risk_usd*USD_TO_INR:.0f})")
        return self.open_trade

    def update(self, current_price):
        """Check if open trade hit SL or TP."""
        if not self.open_trade:
            return None

        t = self.open_trade
        result = None

        if t["direction"] == "BUY":
            if current_price <= t["sl"]:
                result = self._close(current_price, "SL_HIT")
            elif current_price >= t["tp1"]:
                result = self._close(current_price, "TP1_HIT")
        elif t["direction"] == "SELL":
            if current_price >= t["sl"]:
                result = self._close(current_price, "SL_HIT")
            elif current_price <= t["tp1"]:
                result = self._close(current_price, "TP1_HIT")

        return result

    def _close(self, exit_price, reason):
        """Close the open trade and calculate P&L."""
        t = self.open_trade
        if t["direction"] == "BUY":
            pnl_usd = (exit_price - t["entry"]) * t["units"]
        else:
            pnl_usd = (t["entry"] - exit_price) * t["units"]

        pnl_inr    = pnl_usd * USD_TO_INR
        self.balance += pnl_usd
        self.total_pnl += pnl_usd

        if pnl_usd > 0:
            self.wins += 1
        else:
            self.losses += 1

        closed = {**t,
            "exit_price": exit_price,
            "exit_time":  datetime.now(timezone.utc).isoformat(),
            "exit_reason":reason,
            "pnl_usd":    round(pnl_usd, 2),
            "pnl_inr":    round(pnl_inr, 2),
            "balance":    round(self.balance, 2),
        }
        self.trades.append(closed)
        self.open_trade = None

        emoji = "WIN" if pnl_usd > 0 else "LOSS"
        print(f"  [PAPER] CLOSED [{emoji}] @ ${exit_price:.2f} | "
              f"P&L: ${pnl_usd:+.2f} (~₹{pnl_inr:+.0f}) | "
              f"Balance: ${self.balance:.2f} (~₹{self.balance*USD_TO_INR:.0f})")
        return closed

    def stats(self):
        total = self.wins + self.losses
        wr    = (self.wins / total * 100) if total > 0 else 0
        return {
            "balance_usd": round(self.balance, 2),
            "balance_inr": round(self.balance * USD_TO_INR, 0),
            "pnl_usd":     round(self.total_pnl, 2),
            "pnl_inr":     round(self.total_pnl * USD_TO_INR, 0),
            "pnl_pct":     round((self.balance - self.start_bal) / self.start_bal * 100, 2),
            "wins":        self.wins,
            "losses":      self.losses,
            "win_rate":    round(wr, 1),
            "total_trades":total,
        }


# ══════════════════════════════════════════════════════════════════════════════
# LIVE GOLD PRICE (free API)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_gold_price():
    """
    Return the latest XAU/USD price.
    Priority: WebSocket cache → Twelve Data REST → gold-api.com → random stub.
    The WebSocket feed (started at import time) delivers ticks in real-time
    without consuming any REST quota.
    """
    ws_price = _price_ws.get_price()
    if ws_price:
        return ws_price

    # REST fallback — only reached until the WebSocket has its first tick
    try:
        r = requests.get(
            "https://api.twelvedata.com/price",
            params={"symbol": "XAU/USD", "apikey": TWELVE_DATA_KEY},
            timeout=5,
        )
        if r.status_code == 200:
            data = r.json()
            if "price" in data:
                return float(data["price"])
    except Exception:
        pass

    try:
        r = requests.get("https://api.gold-api.com/price/XAU", timeout=5)
        if r.status_code == 200:
            data = r.json()
            return float(data.get("price", data.get("ask", 0)))
    except Exception:
        pass

    base = 3300.0
    return round(base + random.uniform(-15, 15), 2)


def fetch_candles(n=50, tf_minutes=1):
    """
    Fetch XAU/USD OHLCV candles from Twelve Data (symbol: XAU/USD).
    Free tier: 800 requests/day. Intervals: 1min, 5min, 15min, 1h.
    """
    interval_map = {1: "1min", 5: "5min", 15: "15min", 60: "1h"}
    interval = interval_map.get(tf_minutes, "1min")

    try:
        r = requests.get(
            "https://api.twelvedata.com/time_series",
            params={
                "symbol":     "XAU/USD",
                "interval":   interval,
                "outputsize": n,
                "apikey":     TWELVE_DATA_KEY,
            },
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()

        if data.get("status") == "error":
            raise RuntimeError(f"Twelve Data: {data.get('message')}")

        values = data["values"]          # newest first
        df = pd.DataFrame(values[::-1])  # reverse to chronological order
        df = df.rename(columns={"datetime": "time"})
        df[["open", "high", "low", "close"]] = (
            df[["open", "high", "low", "close"]].astype(float).round(2)
        )
        df["volume"] = df["volume"].astype(float) if "volume" in df.columns else 0.0
        df = df[["time", "open", "high", "low", "close", "volume"]].copy()
        return df

    except Exception as e:
        raise RuntimeError(f"Failed to fetch candle data from Twelve Data: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# INDICATORS
# ══════════════════════════════════════════════════════════════════════════════

def compute_indicators(df):
    close = df["close"]

    # RSI(14)
    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    df["rsi"] = (100 - (100 / (1 + gain / loss))).round(2)

    # ATR(14) — measures volatility for SL sizing
    df["tr"]  = pd.concat([
        (df["high"] - df["low"]),
        (df["high"] - close.shift()).abs(),
        (close.shift() - df["low"]).abs()
    ], axis=1).max(axis=1)
    df["atr"] = df["tr"].rolling(14).mean().round(3)

    return df


# ══════════════════════════════════════════════════════════════════════════════
# PRICE ACTION CONTEXT (no named patterns — raw candle structure only)
# ══════════════════════════════════════════════════════════════════════════════

def describe_price_action(df):
    """
    Describe the last few candles in plain price-action terms.
    Returns a short text block — no pattern names, just structure.
    """
    c  = df.iloc[-1]
    p1 = df.iloc[-2]
    p2 = df.iloc[-3] if len(df) >= 3 else p1
    p3 = df.iloc[-4] if len(df) >= 4 else p2

    lines = []

    # --- current candle ---
    body   = abs(c["close"] - c["open"])
    rng    = c["high"] - c["low"]
    wick_lo = min(c["open"], c["close"]) - c["low"]
    wick_hi = c["high"] - max(c["open"], c["close"])
    direction = "bullish" if c["close"] > c["open"] else ("bearish" if c["close"] < c["open"] else "flat")
    strength  = "strong" if (rng > 0 and body / rng > 0.6) else ("weak" if (rng > 0 and body / rng < 0.2) else "moderate")
    lines.append(f"Last candle: {direction}, {strength} body (body ${body:.2f}, range ${rng:.2f})")
    if wick_lo > body * 1.5:
        lines.append(f"  Long lower wick (${wick_lo:.2f}) — buyers pushed back from low")
    if wick_hi > body * 1.5:
        lines.append(f"  Long upper wick (${wick_hi:.2f}) — sellers rejected the high")

    # --- last 3 candle momentum ---
    closes = [p2["close"], p1["close"], c["close"]]
    if closes[2] > closes[1] > closes[0]:
        lines.append("Momentum: 3 consecutive higher closes (bullish pressure)")
    elif closes[2] < closes[1] < closes[0]:
        lines.append("Momentum: 3 consecutive lower closes (bearish pressure)")
    elif closes[2] > closes[1] and closes[1] < closes[0]:
        lines.append("Momentum: pullback then bounce — possible reversal up")
    elif closes[2] < closes[1] and closes[1] > closes[0]:
        lines.append("Momentum: rally then drop — possible reversal down")
    else:
        lines.append("Momentum: mixed, no clear direction")

    # --- recent swing high / low (last 20 candles) ---
    recent = df.tail(20)
    swing_high = recent["high"].max()
    swing_low  = recent["low"].min()
    price      = c["close"]
    lines.append(f"Recent swing high: ${swing_high:.2f} | swing low: ${swing_low:.2f}")
    if price > swing_high * 0.999:
        lines.append("Price is AT or NEAR the recent swing high — watch for rejection")
    elif price < swing_low * 1.001:
        lines.append("Price is AT or NEAR the recent swing low — watch for bounce")
    else:
        dist_to_high = swing_high - price
        dist_to_low  = price - swing_low
        lines.append(f"Price is ${dist_to_low:.2f} above swing low, ${dist_to_high:.2f} below swing high")

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# NEWS FILTER — ForexFactory Economic Calendar (no API key needed)
# ══════════════════════════════════════════════════════════════════════════════

FF_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

NEWS_CACHE = {"fetched_at": None, "headlines": [], "high_impact": False}

# Block trading this many minutes BEFORE and AFTER a USD high-impact event
FF_BLOCK_BEFORE_MIN = 30
FF_BLOCK_AFTER_MIN  = 15

def fetch_news():
    """
    Pull USD high-impact (red-folder) events from the ForexFactory calendar
    (via nfs.faireconomy.media JSON mirror — no API key required).
    Caches for 30 minutes. Sets high_impact=True if any red event falls within
    FF_BLOCK_BEFORE_MIN minutes ahead or FF_BLOCK_AFTER_MIN minutes behind now.
    """
    global NEWS_CACHE

    now_utc = datetime.now(timezone.utc)
    if (NEWS_CACHE["fetched_at"] and
        (now_utc - NEWS_CACHE["fetched_at"]).total_seconds() < 1800):
        return NEWS_CACHE

    try:
        r = requests.get(
            FF_CALENDAR_URL,
            timeout=8,
            headers={"User-Agent": "Mozilla/5.0 (compatible; gold-bot/1.0)"},
        )
        r.raise_for_status()
        events = r.json()

        headlines   = []
        high_impact = False

        for ev in events:
            if ev.get("country", "").upper() != "USD":
                continue
            if ev.get("impact", "").lower() != "high":
                continue

            title    = ev.get("title", "Unknown Event")
            date_raw = ev.get("date", "")

            try:
                # date field is ISO 8601 with tz offset, e.g. "2026-05-13T08:30:00-04:00"
                dt_utc = datetime.fromisoformat(date_raw).astimezone(timezone.utc)
                minutes_away = (dt_utc - now_utc).total_seconds() / 60
                time_label   = dt_utc.strftime("%H:%M UTC")
                label = (f"[USD RED] {title} @ {time_label} "
                         f"({minutes_away:+.0f} min)")
                headlines.append(label)
                if -FF_BLOCK_AFTER_MIN <= minutes_away <= FF_BLOCK_BEFORE_MIN:
                    high_impact = True
            except (ValueError, TypeError):
                headlines.append(f"[USD RED] {title} (time unknown)")

        if not headlines:
            headlines = ["No USD high-impact events found this week"]

        NEWS_CACHE = {
            "fetched_at":  now_utc,
            "headlines":   headlines[:6],
            "high_impact": high_impact,
        }

    except Exception as e:
        NEWS_CACHE = {
            "fetched_at":  now_utc,
            "headlines":   [f"ForexFactory fetch error: {e}"],
            "high_impact": False,
        }

    return NEWS_CACHE


# ══════════════════════════════════════════════════════════════════════════════
# QWEN3 ANALYSIS (via Ollama)
# ══════════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = f"""You are an XAU/USD {'scalp' if MODE == 'scalp' else 'swing'} trader who reads pure price action only.

STRATEGY — small trades, small profits, fast exits:
- Read the raw candle structure: body size, wick direction, momentum of last 3 candles
- BUY when: candle closes bullish with strong body, lower wicks show buyers defending, 3 higher closes
- SELL when: candle closes bearish with strong body, upper wicks show sellers rejecting, 3 lower closes
- WAIT when: candles are mixed, wicks on both sides, no clear pressure from buyers or sellers
- WAIT when: price is right at the recent swing high (for BUY) or swing low (for SELL) — bad R:R
- If high-impact news is active → always WAIT
- {'RSI filter: only BUY if RSI < 65, only SELL if RSI > 35' if MODE == 'scalp' else 'RSI filter: 40–65'}
- Do NOT over-trade. Fewer trades, cleaner setups. When in doubt → WAIT.

You MUST respond with ONLY raw JSON — no markdown, no explanation, no code fences, no <think> tags:
{{
  "signal": "BUY" or "SELL" or "WAIT",
  "direction": "long" or "short" or "none",
  "pattern": "brief candle description, e.g. strong bullish close with lower wick",
  "entry": price as float,
  "sl": stop loss price as float,
  "tp1": take profit 1 as float,
  "tp2": take profit 2 as float,
  "reason": "one sentence max — price action only",
  "confidence": "high" or "medium" or "low",
  "news_clear": true or false
}}"""

def ask_ollama(df, news):
    """Send price action snapshot + news to Qwen3 via Ollama, get trade signal."""
    last5        = df[["time", "open", "high", "low", "close"]].tail(5).round(2).to_dict(orient="records")
    price        = df["close"].iloc[-1]
    rsi          = df["rsi"].iloc[-1]
    atr          = df["atr"].iloc[-1]
    pa_context   = describe_price_action(df)

    if MODE == "scalp":
        sl_buy   = round(price - CFG["sl_pips"], 2)
        tp1_buy  = round(price + CFG["tp1_pips"], 2)
        tp2_buy  = round(price + CFG["tp2_pips"], 2)
        sl_sell  = round(price + CFG["sl_pips"], 2)
        tp1_sell = round(price - CFG["tp1_pips"], 2)
        tp2_sell = round(price - CFG["tp2_pips"], 2)
        levels_text = (
            f"Suggested scalp levels:\n"
            f"  BUY:  Entry ~${price:.2f} | SL ${sl_buy:.2f} | TP1 ${tp1_buy:.2f} | TP2 ${tp2_buy:.2f}\n"
            f"  SELL: Entry ~${price:.2f} | SL ${sl_sell:.2f} | TP1 ${tp1_sell:.2f} | TP2 ${tp2_sell:.2f}"
        )
    else:
        levels_text = (
            f"Setup A levels:\n"
            f"  Entry zone: $4,696–$4,710 | SL $4,670 | TP1 $4,742 | TP2 $4,807"
        )

    prompt = f"""
XAU/USD PRICE ACTION SNAPSHOT — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
Mode: {MODE.upper()} | Timeframe: {CFG['candle_tf']}

Current price: ${price:.2f}
RSI(14):       {rsi}
ATR(14):       ${atr:.2f}

--- PRICE ACTION CONTEXT ---
{pa_context}

--- LAST 5 CANDLES (open, high, low, close) ---
{json.dumps(last5, indent=2)}

--- TRADE LEVELS ---
{levels_text}

--- NEWS ---
High-impact event active: {news['high_impact']}
{chr(10).join('- ' + h for h in news['headlines'][:3])}

Give your trade signal JSON now. /no_think
"""

    resp = ollama.chat(
        model=OLLAMA_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
    )
    raw = resp["message"]["content"]
    # Strip Qwen3 thinking tags if present
    raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL)
    raw = raw.strip().replace("```json", "").replace("```", "").strip()
    return json.loads(raw)


# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════

def log_trade(trade):
    exists = os.path.isfile(LOG_FILE)
    fields = ["id","direction","entry","sl","tp1","tp2","units","open_time",
              "exit_price","exit_time","exit_reason","pnl_usd","pnl_inr",
              "balance","pattern","reason","confidence","news_clear"]
    with open(LOG_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if not exists:
            w.writeheader()
        w.writerow(trade)


def write_bot_state():
    """Write live bot state to bot_state.json so the dashboard can read it."""
    state = {
        "last_cycle_utc": datetime.now(timezone.utc).isoformat(),
        "mode": MODE,
        "tf": CFG["candle_tf"],
        "usd_to_inr": USD_TO_INR,
        "open_trade": trader.open_trade,
        "last_signal": _last_signal,
    }
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, default=str)
    except Exception:
        pass


def print_dashboard(trader, signal):
    """Print a clean terminal dashboard."""
    s = trader.stats()
    print(f"\n{'─'*55}")
    print(f"  PAPER ACCOUNT  |  Mode: {MODE.upper()}")
    print(f"{'─'*55}")
    print(f"  Balance:  ${s['balance_usd']:.2f}  (~₹{s['balance_inr']:,.0f})")
    print(f"  P&L:      ${s['pnl_usd']:+.2f}  (~₹{s['pnl_inr']:+,.0f})  [{s['pnl_pct']:+.2f}%]")
    print(f"  Trades:   {s['total_trades']}  |  Wins: {s['wins']}  |  Losses: {s['losses']}  |  WR: {s['win_rate']}%")
    print(f"{'─'*55}")
    if trader.open_trade:
        t = trader.open_trade
        print(f"  OPEN TRADE: {t['direction']} @ ${t['entry']:.2f} | SL ${t['sl']:.2f} | TP1 ${t['tp1']:.2f}")
    else:
        print(f"  No open trade")
    print(f"{'─'*55}\n")


def send_telegram(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=5)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# MAIN BOT LOOP
# ══════════════════════════════════════════════════════════════════════════════

trader = PaperTrader(balance_usd=PAPER_BALANCE)

def run_bot():
    global _last_signal
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"\n[{now}] Cycle started — Mode: {MODE.upper()}")

    try:
        # 1. Fetch data
        tf_min = 1 if MODE == "scalp" else 15
        df     = fetch_candles(n=50, tf_minutes=tf_min)
        df     = compute_indicators(df)
        price  = df["close"].iloc[-1]

        # 2. Check if open trade hit SL/TP
        closed = trader.update(price)
        if closed:
            log_trade(closed)
            msg = (f"PAPER {closed['exit_reason']}\n"
                   f"{closed['direction']} @ ${closed['entry']:.2f} → ${closed['exit_price']:.2f}\n"
                   f"P&L: ${closed['pnl_usd']:+.2f} (~₹{closed['pnl_inr']:+.0f})\n"
                   f"Balance: ${closed['balance']:.2f} (~₹{closed['balance']*USD_TO_INR:.0f})")
            send_telegram(msg)

        # 3. Skip if already in a trade
        if trader.open_trade:
            print(f"  Price: ${price:.2f} | Monitoring open trade...")
            print_dashboard(trader, None)
            write_bot_state()
            return

        # 4. Fetch news
        news   = fetch_news()
        pa     = describe_price_action(df)
        print(f"  Price: ${price:.2f} | RSI: {df['rsi'].iloc[-1]} | High-impact news: {news['high_impact']}")
        print(f"  Price action:\n    " + pa.replace("\n", "\n    "))

        # 5. Skip if high-impact news
        if news["high_impact"]:
            print(f"  High-impact news detected — skipping cycle.")
            write_bot_state()
            return

        # 6. Ask Qwen3
        signal = ask_ollama(df, news)
        _last_signal = {k: signal.get(k) for k in
            ("signal", "pattern", "reason", "confidence", "entry", "sl", "tp1", "tp2")}
        print(f"  Qwen3: {signal['signal']} | Pattern: {signal.get('pattern')} | "
              f"{signal.get('reason')} [{signal.get('confidence')}]")

        # 7. Open paper trade
        if signal["signal"] in ("BUY", "SELL"):
            trade = trader.open(signal, price)
            risk_usd = trader.balance * RISK_PERCENT
            now_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

            alert = (
                f"\n{'━'*55}\n"
                f"  🔔 TRADE ALERT — {signal['signal']}\n"
                f"{'━'*55}\n"
                f"  Time       : {now_str}\n"
                f"  Direction  : {signal['signal']}\n"
                f"  Entry Price: ${signal['entry']:.2f}\n"
                f"  Stop Loss  : ${signal['sl']:.2f}   "
                f"(risk ${abs(signal['entry']-signal['sl']):.2f}/unit)\n"
                f"  TP1        : ${signal['tp1']:.2f}   "
                f"(reward ${abs(signal['tp1']-signal['entry']):.2f}/unit)\n"
                f"  TP2        : ${signal['tp2']:.2f}   "
                f"(reward ${abs(signal['tp2']-signal['entry']):.2f}/unit)\n"
                f"  Units      : {trade['units']}\n"
                f"  Risk USD   : ${risk_usd:.2f}  (~₹{risk_usd*USD_TO_INR:.0f})\n"
                f"  Pattern    : {signal.get('pattern', '-')}\n"
                f"  Reason     : {signal.get('reason', '-')}\n"
                f"  Confidence : {signal.get('confidence', '-').upper()}\n"
                f"{'━'*55}\n"
            )
            print(alert)

            tg_msg = (
                f"TRADE ALERT — {signal['signal']}\n"
                f"Time:    {now_str}\n"
                f"Entry:   ${signal['entry']:.2f}\n"
                f"SL:      ${signal['sl']:.2f}\n"
                f"TP1:     ${signal['tp1']:.2f}\n"
                f"TP2:     ${signal['tp2']:.2f}\n"
                f"Units:   {trade['units']}\n"
                f"Risk:    ${risk_usd:.2f} (~₹{risk_usd*USD_TO_INR:.0f})\n"
                f"Pattern: {signal.get('pattern', '-')}\n"
                f"Reason:  {signal.get('reason', '-')}\n"
                f"Conf:    {signal.get('confidence', '-').upper()}"
            )
            send_telegram(tg_msg)

        elif signal["signal"] == "WAIT":
            print(f"  Signal: WAIT | Reason: {signal.get('reason', '-')} "
                  f"[{signal.get('confidence', '-')}]")

        print_dashboard(trader, signal)
        write_bot_state()

    except json.JSONDecodeError:
        print("  Qwen3 returned invalid JSON — skipping cycle.")
    except Exception as e:
        print(f"  Error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# DAILY SUMMARY (runs at 23:55 UTC)
# ══════════════════════════════════════════════════════════════════════════════

def daily_summary():
    s = trader.stats()
    summary = (
        f"DAILY PAPER TRADE SUMMARY\n"
        f"Date:      {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n"
        f"Mode:      {MODE.upper()}\n"
        f"Balance:   ${s['balance_usd']:.2f} (~₹{s['balance_inr']:,.0f})\n"
        f"Day P&L:   ${s['pnl_usd']:+.2f} (~₹{s['pnl_inr']:+,.0f}) [{s['pnl_pct']:+.2f}%]\n"
        f"Trades:    {s['total_trades']} | Wins: {s['wins']} | Losses: {s['losses']}\n"
        f"Win rate:  {s['win_rate']}%"
    )
    print(f"\n{'='*50}\n{summary}\n{'='*50}")
    send_telegram(summary)

    exists = os.path.isfile(SUMMARY_FILE)
    with open(SUMMARY_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["date","mode","balance_usd","balance_inr",
                                           "pnl_usd","pnl_inr","pnl_pct",
                                           "total_trades","wins","losses","win_rate"])
        if not exists:
            w.writeheader()
        w.writerow({"date": datetime.now(timezone.utc).strftime("%Y-%m-%d"), "mode": MODE, **s})


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 55)
    print(f"  QWEN3 GOLD PAPER BOT — {MODE.upper()} MODE")
    print("=" * 55)
    print(f"  Ollama model:     {OLLAMA_MODEL}")
    print(f"  Starting balance: ${PAPER_BALANCE} (~₹{PAPER_BALANCE*USD_TO_INR:,.0f})")
    print(f"  Risk per trade:   {RISK_PERCENT*100:.1f}%  (small trades)")
    print(f"  TP1 target:       ${CFG['tp1_pips']} | TP2: ${CFG['tp2_pips']}  (small profits)")
    print(f"  Analysis:         Pure price action — candle structure, swing levels")
    print(f"  News filter:      ON (skips high-impact events)")
    print(f"  Interval:         every {CFG['interval_minutes']} minute(s)")
    print(f"  Log file:         {LOG_FILE}")
    print("=" * 55)
    print("  This is PAPER TRADING — no real money at risk.")
    print("  Press Ctrl+C to stop and see final stats.\n")

    run_bot()

    interval = CFG["interval_minutes"]
    schedule.every(interval).minutes.do(run_bot)
    schedule.every().day.at("23:55").do(daily_summary)

    try:
        while True:
            schedule.run_pending()
            time.sleep(15)
    except KeyboardInterrupt:
        print("\n\nBot stopped. Final stats:")
        daily_summary()

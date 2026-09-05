"""
F&O Option Alert Bot — Scan B only
-------------------------------------
Watches the "next OTM" Call + Put across ALL F&O stocks + indices
(CE = first strike above current price, PE = first strike below).

SCAN B: checks whether the 2:45pm (14:45-15:00) AND 3:00pm (15:00-15:15)
15-minute candles are BOTH green for each contract. Evaluated right after
the 3:00 candle closes, with a short buffer for the last tick to settle —
fires at SCAN_B_CHECK_TIME below (default 3:18pm; change to (15, 20) for
3:20 instead).

Since we only care about two specific candles late in the day, this no
longer needs to run all day — it connects a little before 2:45pm and
disconnects right after sending the alert. Lighter on the server, less
that can go wrong.

Runs on an always-on server (Oracle Cloud, see SETUP_GUIDE.md). Started
fresh each weekday afternoon by a systemd timer.
"""

import os
import time
import socket
import pyotp
import requests
from collections import deque
from datetime import datetime, timedelta, timezone

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# Without this, a network call that never gets a response (or a silently
# blocked connection) can hang forever with no error message. This forces
# any such hang to give up and raise a clear error after 30 seconds instead.
socket.setdefaulttimeout(30)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

ANGEL_API_KEY = os.environ["ANGEL_API_KEY"]
ANGEL_CLIENT_CODE = os.environ["ANGEL_CLIENT_CODE"]
ANGEL_PIN = os.environ["ANGEL_PIN"]
ANGEL_TOTP_SECRET = os.environ["ANGEL_TOTP_SECRET"]

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

IST = timezone(timedelta(hours=5, minutes=30))
CANDLE_MINUTES = 15

# Connect a few minutes before the first candle we care about, so the
# WebSocket + subscriptions are fully warmed up before 2:45pm starts.
WINDOW_START = (14, 40)

# The 3:00 candle (15:00-15:15) finishes at 15:15 — this buffer just lets
# the last tick settle before we evaluate. Change to (15, 20) for 3:20pm.
SCAN_B_CHECK_TIME = (15, 18)

INSTRUMENT_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

# ---------------------------------------------------------------------------
# LOGIN
# ---------------------------------------------------------------------------

def login():
    print("[info] Attempting Angel One login...")
    try:
        smart_api = SmartConnect(api_key=ANGEL_API_KEY)
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        data = smart_api.generateSession(ANGEL_CLIENT_CODE, ANGEL_PIN, totp)
    except (socket.timeout, requests.exceptions.RequestException) as e:
        print(f"[error] Login request timed out or failed at the network level: {e}")
        print("[error] This can mean Angel One's servers didn't respond to this "
              "connection at all — possibly blocking/throttling requests from "
              "GitHub's servers specifically. We'll know for sure once we see "
              "this exact message.")
        raise
    if not data.get("status"):
        raise RuntimeError(f"Angel One login failed: {data}")
    auth_token = data["data"]["jwtToken"]
    feed_token = smart_api.getfeedToken()
    print("[info] Logged in to Angel One successfully.")
    return smart_api, auth_token, feed_token


# ---------------------------------------------------------------------------
# INSTRUMENT MASTER + WATCHLIST BUILDING
# ---------------------------------------------------------------------------

def load_instrument_master():
    print("[info] Downloading instrument master (this is a big file, ~30-60 sec)...")
    r = requests.get(INSTRUMENT_MASTER_URL, timeout=60)
    r.raise_for_status()
    return r.json()


def get_all_fo_underlyings(instrument_master):
    """Auto-discover every underlying (stock or index) with options on
    NFO — this is what makes it 'all F&O stocks' instead of a hardcoded list."""
    names = set(
        r["name"] for r in instrument_master
        if r.get("exch_seg") == "NFO" and r.get("instrumenttype") in ("OPTSTK", "OPTIDX")
    )
    return sorted(names)


def get_spot_ltp(smart_api, name, exch_seg, token):
    try:
        data = smart_api.ltpData(exch_seg, name, token)
        return float(data["data"]["ltp"])
    except Exception as e:
        print(f"[warn] Could not fetch LTP for {name}: {e}")
        return None


def nearest_expiry(option_rows):
    expiries = sorted(set(r["expiry"] for r in option_rows if r.get("expiry")))
    return expiries[0] if expiries else None


def build_watchlist(instrument_master, smart_api):
    """
    For every F&O underlying: find nearest expiry, then pick the "next OTM"
    strike on each side (not ATM):
      - CE watched = smallest strike strictly ABOVE current spot
      - PE watched = largest strike strictly BELOW current spot
    Spot is read right now (~2:40pm), so strikes reflect the afternoon
    price, not a stale morning snapshot.

    Prints progress every 10 underlyings and gives up on any remaining
    ones after a time budget, so a single slow/stuck request can't hang
    the whole job.
    Returns { symboltoken: {"tradingsymbol":..., "underlying":..., "strike":...,
                             "type": "CE"/"PE"} }
    """
    watch_tokens = {}
    underlyings = get_all_fo_underlyings(instrument_master)
    print(f"[info] Discovered {len(underlyings)} F&O underlyings (stocks + indices).")

    start_time = time.time()
    time_budget_secs = 240  # 4 minutes max for building the whole watchlist

    for idx, name in enumerate(underlyings, start=1):
        if idx % 10 == 0 or idx == 1:
            print(f"[info] Building watchlist... {idx}/{len(underlyings)} ({name})")
        if time.time() - start_time > time_budget_secs:
            print(f"[warn] Time budget exceeded while building watchlist — "
                  f"stopping early at {idx}/{len(underlyings)}. "
                  f"Continuing with what we have so far.")
            break

        option_rows = [
            r for r in instrument_master
            if r.get("name") == name and r.get("exch_seg") == "NFO"
            and r.get("instrumenttype") in ("OPTIDX", "OPTSTK")
        ]
        if not option_rows:
            continue

        expiry = nearest_expiry(option_rows)
        chain = [r for r in option_rows if r["expiry"] == expiry]

        underlying_row = next(
            (r for r in instrument_master
             if r.get("name") == name and r.get("exch_seg") in ("NSE", "NFO")
             and r.get("instrumenttype", "") in ("", "AMXIDX", "INDEX")),
            None,
        )
        if not underlying_row:
            print(f"[warn] No spot/index token found for {name} — skipping.")
            continue

        spot = get_spot_ltp(smart_api, underlying_row["symbol"], underlying_row["exch_seg"], underlying_row["token"])
        if spot is None:
            continue

        strikes = sorted(set(float(r["strike"]) / 100 for r in chain))
        if not strikes:
            continue

        strikes_above = [s for s in strikes if s > spot]
        strikes_below = [s for s in strikes if s < spot]
        ce_strike = min(strikes_above) if strikes_above else None
        pe_strike = max(strikes_below) if strikes_below else None

        if ce_strike is None and pe_strike is None:
            print(f"[warn] {name}: no OTM strikes found either side of spot {spot} — skipping.")
            continue

        for r in chain:
            strike_val = float(r["strike"]) / 100
            is_ce_target = strike_val == ce_strike and r["symbol"].endswith("CE")
            is_pe_target = strike_val == pe_strike and r["symbol"].endswith("PE")
            if is_ce_target or is_pe_target:
                opt_type = "CE" if is_ce_target else "PE"
                watch_tokens[r["token"]] = {
                    "tradingsymbol": r["symbol"], "underlying": name,
                    "strike": strike_val, "type": opt_type,
                }

    print(f"[info] Total contracts in watchlist (next-OTM CE+PE across all F&O names): {len(watch_tokens)}")
    return watch_tokens


# ---------------------------------------------------------------------------
# CANDLE BUILDING (only need to remember the last couple of candles)
# ---------------------------------------------------------------------------

class CandleBuilder:
    def __init__(self):
        self.current = {}     # token -> {"start":..., "o":.., "h":.., "l":.., "c":..}
        self.completed = {}   # token -> deque of the last few finished 15-min candles
        self.scan_b_done = False

    def bucket_start(self, dt):
        minute = (dt.minute // CANDLE_MINUTES) * CANDLE_MINUTES
        return dt.replace(minute=minute, second=0, microsecond=0)

    def on_tick(self, token, price, dt):
        bstart = self.bucket_start(dt)
        cur = self.current.get(token)
        if cur is None or cur["start"] != bstart:
            if cur is not None:
                self.completed.setdefault(token, deque(maxlen=3)).append(cur)
            self.current[token] = {"start": bstart, "o": price, "h": price, "l": price, "c": price}
        else:
            cur["h"] = max(cur["h"], price)
            cur["l"] = min(cur["l"], price)
            cur["c"] = price

    def maybe_run_scan_b(self, now, contracts):
        if self.scan_b_done:
            return False
        check_h, check_m = SCAN_B_CHECK_TIME
        if now.hour < check_h or (now.hour == check_h and now.minute < check_m):
            return False
        self.scan_b_done = True  # only ever run once, regardless of outcome

        hits = []
        for token, hist in self.completed.items():
            c_1445 = next((c for c in hist if c["start"].hour == 14 and c["start"].minute == 45), None)
            c_1500 = next((c for c in hist if c["start"].hour == 15 and c["start"].minute == 0), None)
            if c_1445 and c_1500 and c_1445["c"] > c_1445["o"] and c_1500["c"] > c_1500["o"]:
                hits.append((token, c_1445, c_1500))

        send_scan_b_alert(hits, contracts)
        print(f"[info] Scan B evaluated at {now.strftime('%H:%M')} — {len(hits)} contract(s) matched.")
        return True  # signal that we're done and can disconnect


# ---------------------------------------------------------------------------
# ALERTS
# ---------------------------------------------------------------------------

def send_scan_b_alert(hits, contracts):
    if not hits:
        send_telegram(f"Scan B ran at {SCAN_B_CHECK_TIME[0]}:{SCAN_B_CHECK_TIME[1]:02d} — no contracts matched today.")
        return
    lines = [f"*📈 Scan B — 2:45 & 3:00 Candles Both Green ({len(hits)} contracts)*\n"]
    for token, c1, c2 in hits:
        info = contracts.get(token, {})
        lines.append(
            f"{info.get('underlying','?')} {info.get('strike','?')} {info.get('type','?')} "
            f"({info.get('tradingsymbol','?')})\n"
            f"  2:45 O:{c1['o']:.2f} C:{c1['c']:.2f} | 3:00 O:{c2['o']:.2f} C:{c2['c']:.2f}"
        )
    send_telegram("\n".join(lines))
    print(f"[alert] Scan B: sent digest for {len(hits)} contracts.")


def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID, "text": text,
            "parse_mode": "Markdown", "disable_web_page_preview": True,
        }, timeout=10)
        if resp.status_code != 200:
            print(f"[error] Telegram send failed: {resp.text}")
    except Exception as e:
        print(f"[error] Telegram send exception: {e}")


# ---------------------------------------------------------------------------
# TIMING
# ---------------------------------------------------------------------------

def wait_for_window_start():
    while True:
        now = datetime.now(IST)
        start_t = now.replace(hour=WINDOW_START[0], minute=WINDOW_START[1], second=0, microsecond=0)
        if now >= start_t:
            return
        wait_secs = min((start_t - now).total_seconds(), 60)
        print(f"[info] Waiting for the 2:45pm window to start ({wait_secs:.0f}s)...")
        time.sleep(wait_secs)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def stop_feed(sws):
    """Close the WebSocket connection. Tries the documented method first;
    falls back to an alternate in case the exact method name ever drifts
    between SmartApi versions."""
    for attempt in ("close_connection", "close"):
        try:
            getattr(sws, attempt)()
            print(f"[info] Feed closed via sws.{attempt}().")
            return
        except AttributeError:
            continue
        except Exception as e:
            print(f"[warn] sws.{attempt}() raised: {e}")
            return
    print("[warn] Could not find a working close method on sws.")


def main():
    smart_api, auth_token, feed_token = login()

    wait_for_window_start()  # so the watchlist reflects the ~2:40pm price, not the morning's

    instrument_master = load_instrument_master()
    contracts = build_watchlist(instrument_master, smart_api)

    if not contracts:
        print("[error] Watchlist is empty — nothing to monitor.")
        return

    builder = CandleBuilder()
    tokens = list(contracts.keys())
    token_list = [{"exchangeType": 2, "tokens": tokens}]
    sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_CODE, feed_token)

    def on_data(wsapp, message):
        try:
            token = message.get("token")
            ltp = message.get("last_traded_price")
            if token is None or ltp is None or token not in contracts:
                return
            price = float(ltp) / 100.0
            now = datetime.now(IST)
            builder.on_tick(token, price, now)
            done = builder.maybe_run_scan_b(now, contracts)
            if done:
                stop_feed(sws)
        except Exception as e:
            print(f"[warn] on_data error: {e}")

    def on_open(wsapp):
        print("[info] WebSocket connected, subscribing...")
        sws.subscribe("alert-bot", 3, token_list)

    def on_error(wsapp, error):
        print(f"[error] WebSocket error: {error}")

    def on_close(wsapp):
        print("[info] WebSocket closed.")

    sws.on_open = on_open
    sws.on_data = on_data
    sws.on_error = on_error
    sws.on_close = on_close

    print("[info] Starting live feed for the 2:45-3:15 window.")
    send_telegram(
        f"✅ Scan B bot started — watching {len(contracts)} next-OTM contracts across all F&O names.\n"
        f"Will check the 2:45 & 3:00 candles at {SCAN_B_CHECK_TIME[0]}:{SCAN_B_CHECK_TIME[1]:02d}."
    )

    sws.connect()  # blocks until closed
    print("[info] Done for today.")


if __name__ == "__main__":
    main()

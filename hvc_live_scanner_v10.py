#!/usr/bin/env python3
"""
HVC LIVE SCANNER V10 - MT5 DIRECT (12 instruments)
===================================================
Scans 12 instruments for SMC signals in real-time.
Data source: MetaTrader 5 terminal (replaces Twelve Data API).
Inherits all V9 structural fixes:
  1. Swing detection without look-ahead (V3 engine)
  2. Signal lookback = 3 candles (MT5 = near-zero latency)
  3. Persistent zones until invalidation or 2h expiry
  4. 1000 M1 candles = 16.6h data depth

Architecture:
  configs/           # 1 JSON per instrument (from backtest V3)
  state/             # Persistent zone state between scans
  portfolio.json     # Global risk limits

Usage:
    python hvc_live_scanner_v10.py              # Continuous mode
    python hvc_live_scanner_v10.py --once       # Single scan all assets
    python hvc_live_scanner_v10.py --status     # Status overview
    python hvc_live_scanner_v10.py --serve      # With JSON dashboard server
"""

import pandas as pd
import numpy as np
import requests
import json
import os
import sys
import time
import threading
import random
from datetime import datetime, timedelta
from pathlib import Path
from http.server import HTTPServer, SimpleHTTPRequestHandler
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# PATHS
# ============================================================

SCRIPT_DIR = Path(__file__).parent
CONFIGS_DIR = SCRIPT_DIR / "configs"
STATE_DIR = SCRIPT_DIR / "state"

# Ensure dirs exist
CONFIGS_DIR.mkdir(exist_ok=True)
STATE_DIR.mkdir(exist_ok=True)

# ============================================================
# DATA SOURCES
# ============================================================

BINANCE_URL = "https://data-api.binance.vision/api/v3/klines"

# Bridge (single MT5 connection - scanner uses it for both data AND execution)
BRIDGE_BASE_URL = os.environ.get('HVC_BRIDGE_URL', 'http://localhost:8080').rstrip('/')
BRIDGE_SIGNAL_URL = f"{BRIDGE_BASE_URL}/signal"
BRIDGE_CANDLES_URL = f"{BRIDGE_BASE_URL}/candles"
BRIDGE_API_KEY = os.environ.get('HVC_BRIDGE_API_KEY', '')
N8N_WEBHOOK_URL = os.environ.get('HVC_WEBHOOK_URL', 'https://n8n.srv1140766.hstgr.cloud/webhook/hvc-signal')
N8N_RESULT_URL = os.environ.get('HVC_WEBHOOK_RESULT_URL', 'https://n8n.srv1140766.hstgr.cloud/webhook/hvc-result')

# Telegram
TELEGRAM_BOT_TOKEN = os.environ.get('HVC_TELEGRAM_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('HVC_TELEGRAM_CHAT_ID', '')

# ============================================================
# INSTRUMENT REGISTRY
# ============================================================

INSTRUMENTS = {
    # Forex pairs (MT5)
    "AUDUSD": {
        "source": "mt5", "symbol_mt5": "AUDUSD-STD",
        "pip_multiplier": 10000, "spread": 0.8, "slippage_max": 0.3,
        "decimals": 5, "unit": "pips",
        "scan_interval": 60,
        "candles_fetch": 1000,
    },
    "EURUSD": {
        "source": "mt5", "symbol_mt5": "EURUSD-STD",
        "pip_multiplier": 10000, "spread": 0.5, "slippage_max": 0.2,
        "decimals": 5, "unit": "pips",
        "scan_interval": 60,
        "candles_fetch": 1000,
    },
    "GBPUSD": {
        "source": "mt5", "symbol_mt5": "GBPUSD-STD",
        "pip_multiplier": 10000, "spread": 0.8, "slippage_max": 0.3,
        "decimals": 5, "unit": "pips",
        "scan_interval": 60,
        "candles_fetch": 1000,
    },
    "USDJPY": {
        "source": "mt5", "symbol_mt5": "USDJPY-STD",
        "pip_multiplier": 100, "spread": 1.0, "slippage_max": 0.3,
        "decimals": 3, "unit": "pips",
        "scan_interval": 60,
        "candles_fetch": 1000,
    },
    "NZDUSD": {
        "source": "mt5", "symbol_mt5": "NZDUSD-STD",
        "pip_multiplier": 10000, "spread": 1.0, "slippage_max": 0.3,
        "decimals": 5, "unit": "pips",
        "scan_interval": 60,
        "candles_fetch": 1000,
    },
    "EURJPY": {
        "source": "mt5", "symbol_mt5": "EURJPY-STD",
        "pip_multiplier": 100, "spread": 1.5, "slippage_max": 0.5,
        "decimals": 3, "unit": "pips",
        "scan_interval": 60,
        "candles_fetch": 1000,
    },
    "GBPJPY": {
        "source": "mt5", "symbol_mt5": "GBPJPY-STD",
        "pip_multiplier": 100, "spread": 2.0, "slippage_max": 0.5,
        "decimals": 3, "unit": "pips",
        "scan_interval": 60,
        "candles_fetch": 1000,
    },
    "AUDJPY": {
        "source": "mt5", "symbol_mt5": "AUDJPY-STD",
        "pip_multiplier": 100, "spread": 1.5, "slippage_max": 0.5,
        "decimals": 3, "unit": "pips",
        "scan_interval": 60,
        "candles_fetch": 1000,
    },
    "CHFJPY": {
        "source": "mt5", "symbol_mt5": "CHFJPY-STD",
        "pip_multiplier": 100, "spread": 1.5, "slippage_max": 0.5,
        "decimals": 3, "unit": "pips",
        "scan_interval": 60,
        "candles_fetch": 1000,
    },
    "EURGBP": {
        "source": "mt5", "symbol_mt5": "EURGBP-STD",
        "pip_multiplier": 10000, "spread": 0.8, "slippage_max": 0.3,
        "decimals": 5, "unit": "pips",
        "scan_interval": 60,
        "candles_fetch": 1000,
    },
    "XAUUSD": {
        "source": "mt5", "symbol_mt5": "XAUUSD-STD",
        "pip_multiplier": 10, "spread": 3.0, "slippage_max": 1.0,
        "decimals": 2, "unit": "pips",
        "scan_interval": 60,
        "candles_fetch": 1000,
    },
    # Crypto (Binance - free, no rate limit)
    "BTCUSDT": {
        "source": "binance", "symbol_api": "BTCUSDT",
        "pip_multiplier": 1, "spread_pct": 0.01, "slippage_pct": 0.02,
        "fee_pct": 0.10, "decimals": 2, "unit": "USD",
        "scan_interval": 30,  # 30s
        "candles_fetch": 1000,
    },
}

# ============================================================
# PORTFOLIO RISK MANAGEMENT
# ============================================================

PORTFOLIO_LIMITS = {
    "max_total_concurrent_trades": 5,
    "max_concurrent_per_asset": 2,
    "max_correlated_trades": 3,
    "max_daily_loss_r": -5.0,
    "correlation_groups": {
        "usd_long": ["EURUSD_LONG", "GBPUSD_LONG", "NZDUSD_LONG", "AUDUSD_LONG"],
        "usd_short": ["EURUSD_SHORT", "GBPUSD_SHORT", "NZDUSD_SHORT", "AUDUSD_SHORT"],
        "jpy_long": ["USDJPY_SHORT", "EURJPY_SHORT", "GBPJPY_SHORT", "AUDJPY_SHORT", "CHFJPY_SHORT"],
        "jpy_short": ["USDJPY_LONG", "EURJPY_LONG", "GBPJPY_LONG", "AUDJPY_LONG", "CHFJPY_LONG"],
    }
}

# Load portfolio overrides
PORTFOLIO_FILE = CONFIGS_DIR / "portfolio.json"
if PORTFOLIO_FILE.exists():
    try:
        with open(PORTFOLIO_FILE) as f:
            PORTFOLIO_LIMITS.update(json.load(f))
    except Exception:
        pass

# ============================================================
# SCAN SETTINGS
# ============================================================

SIGNAL_LOOKBACK = 3        # V10: reduced from 15 (MT5 data = near-zero latency)
SWING_LOOKBACK = 3
ZONE_MAX_CANDLES = 8       # 8 x 15min = 2h zone lifetime
MAX_TRADE_DURATION = 180   # minutes

MAX_RETRIES = 3
RETRY_DELAY = 5
JSON_SERVER_PORT = 8765

BRIDGE_HANDLES_MONITORING = True  # Bridge position_monitor_loop() handles SL/TP detection

# ============================================================
# STATE
# ============================================================

STATS_FILE = STATE_DIR / "scanner_stats_v10.json"
ACTIVE_TRADES_FILE = STATE_DIR / "active_trades.json"
TRADE_HISTORY_FILE = STATE_DIR / "trade_history.json"
SENT_SIGNALS_FILE = STATE_DIR / "sent_signals.json"

def load_json(filepath, default=None):
    try:
        fp = Path(filepath)
        if fp.exists():
            with open(fp, 'r') as f:
                return json.load(f)
    except Exception:
        pass
    return default if default is not None else {}

def save_json(filepath, data):
    try:
        fp = Path(filepath)
        temp = fp.with_suffix('.tmp')
        with open(temp, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        temp.rename(fp)
    except Exception as e:
        print(f"[WARN] Save error {filepath}: {e}")

def load_stats():
    default = {
        'total_signals': 0,
        'trades_won': 0,
        'trades_lost': 0,
        'total_r': 0.0,
        'last_scan_time': None,
        'last_scan_per_asset': {},
        'signals_per_asset': {},
        'last_daily_recap': None,
        'last_weekly_recap': None,
        'last_monthly_recap': None,
    }
    return load_json(STATS_FILE, default)

stats = load_stats()

# ============================================================
# TRADING PARAMETERS PER ASSET
# ============================================================

def load_asset_config(symbol):
    """Load backtest V3 config for an asset, or use defaults."""
    # Try configs/ directory first
    config_file = CONFIGS_DIR / f"{symbol.lower()}.json"
    if config_file.exists():
        return load_json(config_file)

    # Try V3 backtest output
    v3_config_file = SCRIPT_DIR / f"config_{symbol.lower()}_v3.json"
    if v3_config_file.exists():
        data = load_json(v3_config_file)
        if data and 'params' in data:
            return data

    # Default conservative params
    inst = INSTRUMENTS.get(symbol, {})
    pip_mult = inst.get('pip_multiplier', 10000)

    if pip_mult == 1:  # Crypto
        return {
            'params': {
                'sl': 200, 'rr': 3.0, 'min_wick': 0.50, 'max_zone': 700,
                'short_hours': [14, 15, 16], 'long_hours': [3, 4, 20, 21],
            }
        }
    elif pip_mult == 100:  # JPY pairs
        return {
            'params': {
                'sl': 5.0, 'rr': 3.0, 'min_wick': 0.50, 'max_zone': 20,
                'short_hours': [14, 15], 'long_hours': [3, 17],
            }
        }
    else:  # Standard forex
        return {
            'params': {
                'sl': 3.0, 'rr': 3.0, 'min_wick': 0.50, 'max_zone': 15,
                'short_hours': [14, 15], 'long_hours': [3, 17],
            }
        }


# Pre-load all asset configs
ASSET_PARAMS = {}
for sym in INSTRUMENTS:
    cfg = load_asset_config(sym)
    ASSET_PARAMS[sym] = cfg.get('params', cfg)

# ============================================================
# BRIDGE CONNECTION CHECK
# ============================================================

def bridge_check():
    """Check if bridge is reachable and MT5 is connected."""
    try:
        resp = requests.get(f"{BRIDGE_BASE_URL}/health", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            print(f"[BRIDGE] Connected - Status: {data.get('status')} | Dry-run: {data.get('dry_run')}")
            return True
    except Exception as e:
        print(f"[FATAL] Bridge not reachable at {BRIDGE_BASE_URL}: {e}")
    return False

# ============================================================
# DATA FETCHING
# ============================================================

def fetch_mt5(symbol, outputsize=1000):
    """Fetch M1 candles via bridge HTTP API (avoids MT5 IPC conflicts)."""
    try:
        resp = requests.get(
            BRIDGE_CANDLES_URL,
            params={"symbol": symbol, "count": outputsize},
            timeout=30,
        )
        if resp.status_code != 200:
            print(f"  [{symbol}] Bridge candles error: {resp.status_code} {resp.text[:100]}")
            return None

        data = resp.json()
        candles = data.get("candles", [])
        if not candles:
            print(f"  [{symbol}] Bridge returned 0 candles")
            return None

        df = pd.DataFrame(candles)
        df['datetime'] = pd.to_datetime(df['time'], unit='s')
        df = df.set_index('datetime')
        df = df[['open', 'high', 'low', 'close']].astype(float)

        # Update stats
        stats['last_scan_time'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        stats['last_scan_per_asset'] = stats.get('last_scan_per_asset', {})
        stats['last_scan_per_asset'][symbol] = stats['last_scan_time']

        return df

    except requests.exceptions.ConnectionError:
        print(f"  [{symbol}] Bridge not reachable at {BRIDGE_CANDLES_URL}")
        return None
    except Exception as e:
        print(f"  [{symbol}] Bridge fetch error: {e}")
        return None


def fetch_binance(symbol, outputsize=1000):
    """Fetch M1 data from Binance (free, no API key needed)."""
    inst = INSTRUMENTS[symbol]
    try:
        params = {
            'symbol': inst['symbol_api'],
            'interval': '1m',
            'limit': min(outputsize, 1000),
        }
        response = requests.get(BINANCE_URL, params=params, timeout=15)
        data = response.json()

        if isinstance(data, dict) and 'code' in data:
            print(f"  [{symbol}] Binance error: {data.get('msg', 'Unknown')}")
            return None

        rows = []
        for k in data:
            rows.append({
                'datetime': pd.to_datetime(k[0], unit='ms'),
                'open': float(k[1]),
                'high': float(k[2]),
                'low': float(k[3]),
                'close': float(k[4]),
            })

        df = pd.DataFrame(rows)
        df = df.sort_values('datetime').set_index('datetime')

        stats['last_scan_per_asset'] = stats.get('last_scan_per_asset', {})
        stats['last_scan_per_asset'][symbol] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        return df[['open', 'high', 'low', 'close']]

    except Exception as e:
        print(f"  [{symbol}] Binance fetch error: {e}")
        return None


def fetch_data(symbol, outputsize=None):
    """Fetch M1 candles from appropriate source."""
    inst = INSTRUMENTS[symbol]
    n = outputsize or inst.get('candles_fetch', 1000)

    if inst['source'] == 'binance':
        return fetch_binance(symbol, n)
    elif inst['source'] == 'mt5':
        return fetch_mt5(symbol, n)
    else:
        print(f"  [{symbol}] Unknown source: {inst['source']}")
        return None


# ============================================================
# V3 STRUCTURE DETECTION (from hvc_backtest_v3.py)
# ============================================================

def detect_swings_v3(df, lookback=3):
    """V3: Detect swings using ONLY past data (no look-ahead)."""
    df = df.copy()
    is_sh = pd.Series(True, index=df.index)
    is_sl = pd.Series(True, index=df.index)

    for k in range(1, lookback + 1):
        is_sh = is_sh & (df['high'].shift(k) < df['high']) & (df['high'].shift(-k) < df['high'])
        is_sl = is_sl & (df['low'].shift(k) > df['low']) & (df['low'].shift(-k) > df['low'])

    # Delay confirmation by lookback candles (no future data)
    df['is_sh'] = is_sh.shift(lookback).fillna(False)
    df['is_sl'] = is_sl.shift(lookback).fillna(False)

    df['swing_high_val'] = df['high'].shift(lookback)
    df['swing_low_val'] = df['low'].shift(lookback)

    df['last_sh'] = df['swing_high_val'].where(df['is_sh']).ffill()
    df['last_sl'] = df['swing_low_val'].where(df['is_sl']).ffill()
    df.drop(columns=['swing_high_val', 'swing_low_val'], inplace=True)

    return df


def build_structure_live(df_1m, symbol):
    """
    Build market structure for live scanning.
    Uses V3 logic: no look-ahead swings + persistent zones.
    """
    inst = INSTRUMENTS[symbol]
    pip_mult = inst['pip_multiplier']

    # Resample to 15min
    df_15m = df_1m.resample('15min').agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
    }).dropna()

    if len(df_15m) < 10:
        return df_1m

    # V3 swing detection
    df_15m = detect_swings_v3(df_15m, SWING_LOOKBACK)

    # BOS detection
    df_15m['bos_bear'] = (df_15m['close'] < df_15m['last_sl']) & (df_15m['close'].shift(1) >= df_15m['last_sl'])
    df_15m['bos_bull'] = (df_15m['close'] > df_15m['last_sh']) & (df_15m['close'].shift(1) <= df_15m['last_sh'])

    # V3: Build persistent zone registry
    tf_minutes = 15
    max_zone_minutes = ZONE_MAX_CANDLES * tf_minutes  # 2h

    zones = []
    for idx in df_15m.index:
        row = df_15m.loc[idx]
        if row['bos_bear']:
            zh, zl = row['last_sh'], row['last_sl']
            if pd.notna(zh) and pd.notna(zl) and zh > zl:
                zones.append({
                    'high': zh, 'low': zl, 'dir': 'BEAR',
                    'created_at': idx,
                    'expires_at': idx + pd.Timedelta(minutes=max_zone_minutes),
                    'touches': 0,
                })
        if row['bos_bull']:
            zh, zl = row['last_sh'], row['last_sl']
            if pd.notna(zh) and pd.notna(zl) and zh > zl:
                zones.append({
                    'high': zh, 'low': zl, 'dir': 'BULL',
                    'created_at': idx,
                    'expires_at': idx + pd.Timedelta(minutes=max_zone_minutes),
                    'touches': 0,
                })

    # Propagate zones to M1
    df_1m = df_1m.copy()
    df_1m['zone_high'] = np.nan
    df_1m['zone_low'] = np.nan
    df_1m['zone_dir'] = None

    active_zones = []
    zone_idx = 0
    zones.sort(key=lambda z: z['created_at'])

    for m1_idx in df_1m.index:
        while zone_idx < len(zones) and zones[zone_idx]['created_at'] <= m1_idx:
            active_zones.append(zones[zone_idx])
            zone_idx += 1

        surviving = []
        for z in active_zones:
            if m1_idx > z['expires_at']:
                continue
            if z['dir'] == 'BEAR' and df_1m.loc[m1_idx, 'high'] > z['high']:
                continue
            if z['dir'] == 'BULL' and df_1m.loc[m1_idx, 'low'] < z['low']:
                continue
            if z['touches'] >= 3:
                continue
            surviving.append(z)
        active_zones = surviving

        best_zone = None
        for z in reversed(active_zones):
            if z['dir'] == 'BEAR' and z['low'] <= df_1m.loc[m1_idx, 'high'] <= z['high']:
                best_zone = z
                break
            if z['dir'] == 'BULL' and z['low'] <= df_1m.loc[m1_idx, 'low'] <= z['high']:
                best_zone = z
                break

        if best_zone:
            df_1m.loc[m1_idx, 'zone_high'] = best_zone['high']
            df_1m.loc[m1_idx, 'zone_low'] = best_zone['low']
            df_1m.loc[m1_idx, 'zone_dir'] = best_zone['dir']
            best_zone['touches'] += 1

    # M1 indicators
    df_1m['range'] = df_1m['high'] - df_1m['low']
    df_1m['upper_wick'] = df_1m['high'] - df_1m[['open', 'close']].max(axis=1)
    df_1m['lower_wick'] = df_1m[['open', 'close']].min(axis=1) - df_1m['low']
    df_1m['upper_wick_pct'] = np.where(df_1m['range'] > 0, df_1m['upper_wick'] / df_1m['range'], 0)
    df_1m['lower_wick_pct'] = np.where(df_1m['range'] > 0, df_1m['lower_wick'] / df_1m['range'], 0)
    df_1m['is_bearish'] = df_1m['close'] < df_1m['open']
    df_1m['is_bullish'] = df_1m['close'] > df_1m['open']
    df_1m['hour'] = df_1m.index.hour

    # RSI
    delta = df_1m['close'].diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    df_1m['rsi'] = 100 - (100 / (1 + gain / loss))

    # Zone size in pips/USD
    df_1m['zone_size'] = (df_1m['zone_high'] - df_1m['zone_low']) * pip_mult

    # HTF trend (from the 15M resampled to 1H EMA)
    df_1h = df_1m.resample('1h').agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
    }).dropna()

    if len(df_1h) >= 50:
        df_1h['ema_20'] = df_1h['close'].ewm(span=20).mean()
        df_1h['ema_50'] = df_1h['close'].ewm(span=50).mean()
        df_1h['trend'] = np.where(df_1h['ema_20'] > df_1h['ema_50'], 'UP', 'DOWN')
    else:
        df_1h['trend'] = 'NEUTRAL'

    df_1m['htf_trend'] = ''
    for idx_1h in df_1h.index:
        start = idx_1h
        end = idx_1h + pd.Timedelta(hours=1)
        mask = (df_1m.index >= start) & (df_1m.index < end)
        if mask.any():
            df_1m.loc[mask, 'htf_trend'] = df_1h.loc[idx_1h, 'trend']

    # Save active zones count to state for dashboard
    n_active = len(active_zones)
    zone_state = {
        'symbol': symbol,
        'active_zones': n_active,
        'total_zones_found': len(zones),
        'updated_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
    }
    save_json(STATE_DIR / f"zones_{symbol.lower()}.json", zone_state)

    return df_1m


# ============================================================
# SIGNAL DETECTION
# ============================================================

def detect_signals_live(df_1m, symbol):
    """Detect trading signals for a given instrument."""
    params = ASSET_PARAMS.get(symbol, {})
    inst = INSTRUMENTS[symbol]
    pip_mult = inst['pip_multiplier']

    short_hours = params.get('short_hours', [14, 15])
    long_hours = params.get('long_hours', [3, 17])
    min_wick = params.get('min_wick', 0.50)
    max_zone = params.get('max_zone', 15)

    signals = []
    sent = load_json(SENT_SIGNALS_FILE, {})

    # V10: Check last 3 M1 candles (MT5 = near-zero latency)
    lookback = min(SIGNAL_LOOKBACK, len(df_1m))

    for i in range(-lookback, 0):
        if abs(i) > len(df_1m):
            continue
        row = df_1m.iloc[i]
        ts = row.name.strftime('%Y-%m-%d %H:%M:%S')
        hour = row['hour']

        # Skip if already sent
        for direction in ['SHORT', 'LONG']:
            key = f"{symbol}_{direction}_{ts}"
            if key in sent:
                continue

            # Check signal conditions
            if direction == 'SHORT':
                if hour not in short_hours:
                    continue
                if row['zone_dir'] != 'BEAR':
                    continue
                if not (row['high'] >= row['zone_low'] and row['high'] <= row['zone_high']):
                    continue
                if row['upper_wick_pct'] < min_wick:
                    continue
                if not row['is_bearish']:
                    continue
            else:  # LONG
                if hour not in long_hours:
                    continue
                if row['zone_dir'] != 'BULL':
                    continue
                if not (row['low'] >= row['zone_low'] and row['low'] <= row['zone_high']):
                    continue
                if row['lower_wick_pct'] < min_wick:
                    continue
                if not row['is_bullish']:
                    continue

            # Zone size check
            zone_size = row['zone_size']
            if pd.isna(zone_size) or zone_size <= 0 or zone_size > max_zone:
                continue

            # Anti-consecutive: check previous candle
            if abs(i) > 1:
                prev_row = df_1m.iloc[i - 1]
                if direction == 'SHORT' and prev_row.get('zone_dir') == 'BEAR':
                    prev_wick = prev_row.get('upper_wick_pct', 0)
                    if prev_wick >= min_wick and prev_row.get('is_bearish', False):
                        continue
                elif direction == 'LONG' and prev_row.get('zone_dir') == 'BULL':
                    prev_wick = prev_row.get('lower_wick_pct', 0)
                    if prev_wick >= min_wick and prev_row.get('is_bullish', False):
                        continue

            # Format signal
            signal = format_signal(row, direction, symbol, params, inst)
            signals.append(signal)

            # Mark as sent
            sent[key] = {'ts': ts, 'symbol': symbol}
            save_json(SENT_SIGNALS_FILE, sent)

    return signals


def format_signal(row, direction, symbol, params, inst):
    """Format a signal for webhook/telegram."""
    entry_price = row['close']
    pip_mult = inst['pip_multiplier']
    sl_amount = params.get('sl', 3.0)
    rr = params.get('rr', 3.0)
    tp_amount = sl_amount * rr
    decimals = inst.get('decimals', 5)

    sl_dist = sl_amount / pip_mult
    tp_dist = tp_amount / pip_mult

    if direction == 'SHORT':
        sl_price = entry_price + sl_dist
        tp_price = entry_price - tp_dist
    else:
        sl_price = entry_price - sl_dist
        tp_price = entry_price + tp_dist

    entry_time = row.name
    tahiti_time = entry_time - timedelta(hours=10)

    return {
        'symbol': symbol,
        'direction': direction,
        'entry_price': round(entry_price, decimals),
        'sl_price': round(sl_price, decimals),
        'tp_price': round(tp_price, decimals),
        'sl_amount': sl_amount,
        'tp_amount': tp_amount,
        'rr': rr,
        'wick_pct': round(
            (row['upper_wick_pct'] if direction == 'SHORT' else row['lower_wick_pct']) * 100, 1
        ),
        'zone_size': round(row['zone_size'], 1),
        'rsi': round(row['rsi'], 1) if pd.notna(row.get('rsi')) else None,
        'htf_trend': row.get('htf_trend', ''),
        'hour_utc': int(row['hour']),
        'timestamp_utc': entry_time.strftime('%Y-%m-%d %H:%M:%S'),
        'timestamp_tahiti': tahiti_time.strftime('%Y-%m-%d %H:%M:%S'),
        'source': 'HVC_SCANNER_V10',
    }


# ============================================================
# PORTFOLIO RISK CHECKS
# ============================================================

def check_portfolio_risk(signal):
    """Check if opening this trade respects portfolio limits."""
    active = load_json(ACTIVE_TRADES_FILE, [])
    limits = PORTFOLIO_LIMITS

    # Max total concurrent
    if len(active) >= limits['max_total_concurrent_trades']:
        return False, f"Max concurrent trades ({limits['max_total_concurrent_trades']})"

    # Max per asset
    asset_count = sum(1 for t in active if t.get('symbol') == signal['symbol'])
    if asset_count >= limits['max_concurrent_per_asset']:
        return False, f"Max trades for {signal['symbol']} ({limits['max_concurrent_per_asset']})"

    # Max correlated
    trade_key = f"{signal['symbol']}_{signal['direction']}"
    for group_name, members in limits.get('correlation_groups', {}).items():
        if trade_key in members:
            corr_count = sum(
                1 for t in active
                if f"{t.get('symbol')}_{t.get('direction')}" in members
            )
            if corr_count >= limits['max_correlated_trades']:
                return False, f"Max correlated trades ({group_name}: {limits['max_correlated_trades']})"

    # Daily loss limit
    today = datetime.utcnow().strftime('%Y-%m-%d')
    history = load_json(TRADE_HISTORY_FILE, [])
    today_pnl = sum(
        t.get('pnl_r', 0) for t in history
        if t.get('close_time', '').startswith(today)
    )
    if today_pnl <= limits.get('max_daily_loss_r', -5.0):
        return False, f"Daily loss limit reached ({today_pnl:.1f}R)"

    return True, "OK"


# ============================================================
# TRADE MANAGEMENT
# ============================================================

def check_active_trades(symbol, df_1m):
    """Check active trades for SL/TP. Skipped when bridge handles monitoring."""
    if BRIDGE_HANDLES_MONITORING:
        return []

    trades = load_json(ACTIVE_TRADES_FILE, [])
    symbol_trades = [t for t in trades if t.get('symbol') == symbol]
    other_trades = [t for t in trades if t.get('symbol') != symbol]

    if not symbol_trades:
        return []

    closed_trades = []
    still_open = []

    for trade in symbol_trades:
        entry_time = datetime.strptime(trade['entry_time'], '%Y-%m-%d %H:%M:%S')
        mask = df_1m.index > entry_time
        if not mask.any():
            still_open.append(trade)
            continue

        recent = df_1m[mask]
        trade_closed = False

        for idx in recent.index:
            candle = recent.loc[idx]
            close_time = idx.to_pydatetime() if hasattr(idx, 'to_pydatetime') else idx
            duration_min = int((close_time - entry_time).total_seconds() / 60)

            if trade['direction'] == 'SHORT':
                if candle['high'] >= trade['sl_price']:
                    close_price = trade['sl_price']
                    pnl_r = -1.0
                    status = 'SL'
                    trade_closed = True
                elif candle['low'] <= trade['tp_price']:
                    close_price = trade['tp_price']
                    pnl_r = trade.get('rr', 3.0)
                    status = 'TP'
                    trade_closed = True
            else:  # LONG
                if candle['low'] <= trade['sl_price']:
                    close_price = trade['sl_price']
                    pnl_r = -1.0
                    status = 'SL'
                    trade_closed = True
                elif candle['high'] >= trade['tp_price']:
                    close_price = trade['tp_price']
                    pnl_r = trade.get('rr', 3.0)
                    status = 'TP'
                    trade_closed = True

            # Timeout
            if not trade_closed and duration_min >= MAX_TRADE_DURATION:
                close_price = candle['close']
                inst = INSTRUMENTS.get(symbol, {})
                pip_mult = inst.get('pip_multiplier', 10000)
                sl_amount = trade.get('sl_amount', 3.0)
                if trade['direction'] == 'SHORT':
                    pnl_raw = (trade['entry_price'] - close_price) * pip_mult
                else:
                    pnl_raw = (close_price - trade['entry_price']) * pip_mult
                pnl_r = round(pnl_raw / sl_amount, 2) if sl_amount > 0 else 0
                status = 'TIMEOUT'
                trade_closed = True

            if trade_closed:
                closed_entry = {
                    **trade,
                    'status': status,
                    'close_price': round(close_price, trade.get('decimals', 5)),
                    'close_time': close_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'pnl_r': round(pnl_r, 2),
                    'duration_minutes': duration_min,
                }
                closed_trades.append(closed_entry)

                emoji = '+' if status == 'TP' else '-'
                print(f"  [{symbol}] {emoji} {status}: {trade['direction']} @ {trade['entry_price']} | {pnl_r:+.1f}R | {duration_min}min")
                break

        if not trade_closed:
            still_open.append(trade)

    # Save updated trades
    save_json(ACTIVE_TRADES_FILE, other_trades + still_open)

    # Save to history
    if closed_trades:
        history = load_json(TRADE_HISTORY_FILE, [])
        history.extend(closed_trades)
        save_json(TRADE_HISTORY_FILE, history)

        # Update stats
        global stats
        for ct in closed_trades:
            if ct['status'] == 'TP':
                stats['trades_won'] = stats.get('trades_won', 0) + 1
            elif ct['status'] == 'SL':
                stats['trades_lost'] = stats.get('trades_lost', 0) + 1
            stats['total_r'] = stats.get('total_r', 0) + ct['pnl_r']
        save_json(STATS_FILE, stats)

    return closed_trades


def add_active_trade(signal):
    """Add a new active trade."""
    trades = load_json(ACTIVE_TRADES_FILE, [])
    inst = INSTRUMENTS.get(signal['symbol'], {})

    trade = {
        'id': f"{signal['symbol']}_{signal['direction']}_{signal['timestamp_utc'].replace(' ', '_').replace(':', '-')}",
        'symbol': signal['symbol'],
        'direction': signal['direction'],
        'entry_price': signal['entry_price'],
        'sl_price': signal['sl_price'],
        'tp_price': signal['tp_price'],
        'sl_amount': signal['sl_amount'],
        'rr': signal['rr'],
        'decimals': inst.get('decimals', 5),
        'entry_time': signal['timestamp_utc'],
    }

    trades.append(trade)
    save_json(ACTIVE_TRADES_FILE, trades)
    return trade


# ============================================================
# WEBHOOKS & NOTIFICATIONS
# ============================================================

def send_webhook(signal):
    """Send signal to MT5 bridge (execution) and n8n (Telegram notifications)."""
    # 1. Send to MT5 bridge for execution
    if BRIDGE_SIGNAL_URL:
        try:
            headers = {'Content-Type': 'application/json'}
            if BRIDGE_API_KEY:
                headers['X-API-Key'] = BRIDGE_API_KEY
            resp = requests.post(BRIDGE_SIGNAL_URL, json=signal, headers=headers, timeout=15)
            if resp.status_code == 200:
                result = resp.json()
                if result.get('success'):
                    print(f"    [BRIDGE] Executed: ticket={result.get('ticket')} price={result.get('price')}")
                elif result.get('stale_reject'):
                    print(f"    [BRIDGE] Stale signal rejected: {result.get('error')}")
                else:
                    print(f"    [BRIDGE] Rejected: {result.get('error')}")
            else:
                print(f"    [BRIDGE] HTTP {resp.status_code}")
        except Exception as e:
            print(f"    [BRIDGE] Error: {e}")

    # 2. Send to n8n for Telegram notification
    if N8N_WEBHOOK_URL:
        try:
            resp = requests.post(N8N_WEBHOOK_URL, json=signal, timeout=10)
        except Exception as e:
            print(f"    [N8N] Error: {e}")


def send_result_webhook(closed_trade):
    """Send trade result to n8n."""
    try:
        total = stats.get('trades_won', 0) + stats.get('trades_lost', 0)
        wr = (stats.get('trades_won', 0) / total * 100) if total > 0 else 0

        payload = {
            'type': 'TRADE_RESULT',
            'trade_id': closed_trade.get('id', 'unknown'),
            'symbol': closed_trade['symbol'],
            'direction': closed_trade['direction'],
            'result': closed_trade['status'],
            'entry_price': closed_trade['entry_price'],
            'close_price': closed_trade['close_price'],
            'pnl_r': closed_trade['pnl_r'],
            'duration_minutes': closed_trade['duration_minutes'],
            'stats': {
                'total': total, 'won': stats.get('trades_won', 0),
                'lost': stats.get('trades_lost', 0),
                'total_r': round(stats.get('total_r', 0), 1),
                'win_rate': round(wr, 1),
            }
        }
        response = requests.post(N8N_RESULT_URL, json=payload, timeout=15)
        return response.status_code == 200
    except Exception:
        return False


def send_telegram(message):
    """Send message to Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': message,
            'parse_mode': 'HTML',
        }
        response = requests.post(url, json=payload, timeout=10)
        return response.status_code == 200
    except Exception:
        return False


# ============================================================
# TELEGRAM RECAPS (DAILY / WEEKLY / MONTHLY)
# ============================================================

RECAP_HOUR = 22   # 22:00 UTC = midi Tahiti (UTC-10)
RECAP_WINDOW = 5  # minutes window to catch it

MONTH_NAMES_FR = {
    1: 'Janvier', 2: 'Fevrier', 3: 'Mars', 4: 'Avril',
    5: 'Mai', 6: 'Juin', 7: 'Juillet', 8: 'Aout',
    9: 'Septembre', 10: 'Octobre', 11: 'Novembre', 12: 'Decembre'
}
DAY_NAMES_FR = ['Lun', 'Mar', 'Mer', 'Jeu', 'Ven', 'Sam', 'Dim']


def filter_trades_by_range(history, start_dt, end_dt):
    """Filter closed trades where close_time is within [start_dt, end_dt]."""
    filtered = []
    for t in history:
        ct = t.get('close_time')
        if not ct:
            continue
        try:
            close_dt = datetime.strptime(ct, '%Y-%m-%d %H:%M:%S')
            if start_dt <= close_dt <= end_dt:
                filtered.append(t)
        except Exception:
            continue
    return filtered


def get_week_bounds(dt):
    """Returns (monday 00:00, friday 23:59:59) UTC for the week containing dt."""
    monday = dt - timedelta(days=dt.weekday())
    friday = monday + timedelta(days=4)
    return (
        monday.replace(hour=0, minute=0, second=0, microsecond=0),
        friday.replace(hour=23, minute=59, second=59, microsecond=0)
    )


def compute_recap_stats(trades):
    """Compute recap metrics from a list of closed trades."""
    tp = [t for t in trades if t.get('status') == 'TP']
    sl = [t for t in trades if t.get('status') == 'SL']
    to = [t for t in trades if t.get('status') == 'TIMEOUT']
    total = len(tp) + len(sl)
    wr = (len(tp) / total * 100) if total > 0 else 0
    total_r = sum(t.get('pnl_r', 0) for t in trades)

    best = max(trades, key=lambda t: t.get('pnl_r', 0)) if trades else None
    worst = min(trades, key=lambda t: t.get('pnl_r', 0)) if trades else None

    # Per-instrument breakdown
    by_inst = {}
    for t in trades:
        sym = t.get('symbol', '?')
        if sym not in by_inst:
            by_inst[sym] = {'trades': 0, 'tp': 0, 'sl': 0, 'r': 0.0}
        by_inst[sym]['trades'] += 1
        if t.get('status') == 'TP':
            by_inst[sym]['tp'] += 1
        elif t.get('status') == 'SL':
            by_inst[sym]['sl'] += 1
        by_inst[sym]['r'] += t.get('pnl_r', 0)

    return {
        'total': len(trades), 'tp': len(tp), 'sl': len(sl), 'timeout': len(to),
        'wr': wr, 'total_r': total_r,
        'best': best, 'worst': worst,
        'by_instrument': by_inst,
    }


def generate_daily_recap(date_str):
    """Generate daily recap message for a given date (YYYY-MM-DD)."""
    history = load_json(TRADE_HISTORY_FILE, [])
    active = load_json(ACTIVE_TRADES_FILE, [])

    start = datetime.strptime(date_str, '%Y-%m-%d')
    end = start.replace(hour=23, minute=59, second=59)
    trades = filter_trades_by_range(history, start, end)

    day_name = DAY_NAMES_FR[start.weekday()]
    day_label = f"{day_name} {start.day} {MONTH_NAMES_FR[start.month]}"

    if not trades:
        msg = (
            f"<b>HVC Scanner - Recap {day_label}</b>\n\n"
            f"Aucun trade cloture aujourd'hui.\n"
            f"Signaux envoyes: {stats.get('signals_per_asset', {})}\n"
        )
        if active:
            msg += f"\nTrades actifs: {len(active)}"
        return msg

    s = compute_recap_stats(trades)

    lines = [f"<b>HVC Scanner - Recap {day_label}</b>\n"]
    lines.append(f"Trades: {s['total']} (TP: {s['tp']} | SL: {s['sl']}" +
                 (f" | TO: {s['timeout']}" if s['timeout'] else "") + ")")
    lines.append(f"Win Rate: <code>{s['wr']:.0f}%</code> | Total: <code>{s['total_r']:+.1f}R</code>\n")

    # Trade list
    for t in trades:
        sym = t.get('symbol', '?')
        d = t.get('direction', '?')
        r = t.get('pnl_r', 0)
        dur = t.get('duration_minutes', 0)
        status = t.get('status', '?')
        icon = '+' if status == 'TP' else '-' if status == 'SL' else '~'
        lines.append(f"{icon} {sym} {d} <code>{r:+.1f}R</code> ({dur}min)")

    # Per instrument if > 3 trades
    if s['total'] >= 3 and len(s['by_instrument']) > 1:
        lines.append("\n<b>Par instrument:</b>")
        for sym, data in sorted(s['by_instrument'].items(), key=lambda x: x[1]['r'], reverse=True):
            lines.append(f"  {sym}: {data['trades']}t ({data['tp']}W/{data['sl']}L) <code>{data['r']:+.1f}R</code>")

    if active:
        lines.append(f"\nTrades actifs: {len(active)}")
        for t in active:
            entry_time = t.get('entry_time', '?')
            lines.append(f"  {t.get('symbol','?')} {t.get('direction','?')} depuis {entry_time[11:16]}")

    return '\n'.join(lines)


def generate_weekly_recap(monday_dt, friday_dt):
    """Generate weekly recap for Monday-Friday range."""
    history = load_json(TRADE_HISTORY_FILE, [])
    trades = filter_trades_by_range(history, monday_dt, friday_dt)

    week_label = f"{monday_dt.day}-{friday_dt.day} {MONTH_NAMES_FR[friday_dt.month]}"

    if not trades:
        return f"<b>HVC Scanner - Recap Semaine {week_label}</b>\n\nAucun trade cette semaine."

    s = compute_recap_stats(trades)

    lines = [f"<b>HVC Scanner - Recap Semaine</b>"]
    lines.append(f"{week_label}\n")
    lines.append(f"Trades: {s['total']} (TP: {s['tp']} | SL: {s['sl']}" +
                 (f" | TO: {s['timeout']}" if s['timeout'] else "") + ")")
    lines.append(f"Win Rate: <code>{s['wr']:.0f}%</code> | Total: <code>{s['total_r']:+.1f}R</code>\n")

    # Daily breakdown
    lines.append("<b>Par jour:</b>")
    best_day = None
    worst_day = None
    best_day_r = -999
    worst_day_r = 999

    for d in range(5):  # Mon-Fri
        day_dt = monday_dt + timedelta(days=d)
        day_end = day_dt.replace(hour=23, minute=59, second=59)
        day_trades = filter_trades_by_range(history, day_dt, day_end)
        day_r = sum(t.get('pnl_r', 0) for t in day_trades)
        day_name = DAY_NAMES_FR[d]
        n = len(day_trades)
        if n > 0:
            lines.append(f"  {day_name}: {n}t <code>{day_r:+.1f}R</code>")
            if day_r > best_day_r:
                best_day_r = day_r
                best_day = day_name
            if day_r < worst_day_r:
                worst_day_r = day_r
                worst_day = day_name
        else:
            lines.append(f"  {day_name}: -")

    if best_day:
        lines.append(f"\nMeilleur: {best_day} (<code>{best_day_r:+.1f}R</code>)")
    if worst_day and worst_day != best_day:
        lines.append(f"Pire: {worst_day} (<code>{worst_day_r:+.1f}R</code>)")

    # Per instrument
    if len(s['by_instrument']) > 1:
        lines.append("\n<b>Par instrument:</b>")
        for sym, data in sorted(s['by_instrument'].items(), key=lambda x: x[1]['r'], reverse=True):
            lines.append(f"  {sym}: {data['trades']}t ({data['tp']}W/{data['sl']}L) <code>{data['r']:+.1f}R</code>")

    # Compare to last week
    prev_monday = monday_dt - timedelta(days=7)
    prev_friday = friday_dt - timedelta(days=7)
    prev_trades = filter_trades_by_range(history, prev_monday, prev_friday)
    if prev_trades:
        prev_r = sum(t.get('pnl_r', 0) for t in prev_trades)
        diff = s['total_r'] - prev_r
        lines.append(f"\nvs semaine prec: <code>{diff:+.1f}R</code> ({len(prev_trades)}t → {s['total']}t)")

    return '\n'.join(lines)


def generate_monthly_recap(year, month):
    """Generate monthly recap."""
    history = load_json(TRADE_HISTORY_FILE, [])

    first_day = datetime(year, month, 1)
    if month == 12:
        last_day = datetime(year, 12, 31, 23, 59, 59)
    else:
        last_day = datetime(year, month + 1, 1) - timedelta(seconds=1)

    trades = filter_trades_by_range(history, first_day, last_day)
    month_label = f"{MONTH_NAMES_FR[month]} {year}"

    if not trades:
        return f"<b>HVC Scanner - Recap {month_label}</b>\n\nAucun trade ce mois."

    s = compute_recap_stats(trades)

    lines = [f"<b>HVC Scanner - Recap Mensuel</b>"]
    lines.append(f"{month_label}\n")
    lines.append(f"Trades: {s['total']} (TP: {s['tp']} | SL: {s['sl']}" +
                 (f" | TO: {s['timeout']}" if s['timeout'] else "") + ")")
    lines.append(f"Win Rate: <code>{s['wr']:.0f}%</code> | Total: <code>{s['total_r']:+.1f}R</code>\n")

    # Weekly breakdown
    lines.append("<b>Par semaine:</b>")
    current = first_day
    week_num = 1
    while current <= last_day:
        week_start = current
        week_end = min(current + timedelta(days=6), last_day)
        week_end = week_end.replace(hour=23, minute=59, second=59)
        wk_trades = filter_trades_by_range(history, week_start, week_end)
        wk_r = sum(t.get('pnl_r', 0) for t in wk_trades)
        n = len(wk_trades)
        if n > 0:
            lines.append(f"  S{week_num} ({week_start.day}-{week_end.day}): {n}t <code>{wk_r:+.1f}R</code>")
        else:
            lines.append(f"  S{week_num} ({week_start.day}-{week_end.day}): -")
        current = week_end + timedelta(seconds=1)
        current = current.replace(hour=0, minute=0, second=0)
        week_num += 1

    # Per instrument
    if len(s['by_instrument']) > 1:
        lines.append("\n<b>Par instrument:</b>")
        for sym, data in sorted(s['by_instrument'].items(), key=lambda x: x[1]['r'], reverse=True)[:8]:
            lines.append(f"  {sym}: {data['trades']}t ({data['tp']}W/{data['sl']}L) <code>{data['r']:+.1f}R</code>")

    # All-time stats
    all_r = sum(t.get('pnl_r', 0) for t in history)
    all_tp = len([t for t in history if t.get('status') == 'TP'])
    all_total = len([t for t in history if t.get('status') in ('TP', 'SL')])
    all_wr = (all_tp / all_total * 100) if all_total > 0 else 0
    lines.append(f"\n<b>All-time:</b>")
    lines.append(f"  {len(history)} trades | WR: <code>{all_wr:.0f}%</code> | <code>{all_r:+.1f}R</code>")

    # Compare to last month
    if month == 1:
        prev_year, prev_month = year - 1, 12
    else:
        prev_year, prev_month = year, month - 1
    prev_first = datetime(prev_year, prev_month, 1)
    if prev_month == 12:
        prev_last = datetime(prev_year, 12, 31, 23, 59, 59)
    else:
        prev_last = datetime(prev_year, prev_month + 1, 1) - timedelta(seconds=1)
    prev_trades = filter_trades_by_range(history, prev_first, prev_last)
    if prev_trades:
        prev_r = sum(t.get('pnl_r', 0) for t in prev_trades)
        diff = s['total_r'] - prev_r
        lines.append(f"\nvs mois prec: <code>{diff:+.1f}R</code> ({len(prev_trades)}t → {s['total']}t)")

    return '\n'.join(lines)


def check_and_send_recaps():
    """Check if it's time to send daily/weekly/monthly recaps. Called every scan cycle."""
    global stats
    now = datetime.utcnow()

    # Only trigger in the RECAP_HOUR window
    if now.hour != RECAP_HOUR or now.minute >= RECAP_WINDOW:
        return

    today_str = now.strftime('%Y-%m-%d')

    # --- DAILY RECAP ---
    last_daily = stats.get('last_daily_recap')
    if not last_daily or not last_daily.startswith(today_str):
        print(f"[RECAP] Generating daily recap for {today_str}...")
        msg = generate_daily_recap(today_str)
        if send_telegram(msg):
            stats['last_daily_recap'] = now.strftime('%Y-%m-%d %H:%M:%S')
            save_json(STATS_FILE, stats)
            print(f"[RECAP] Daily recap sent.")
        else:
            print(f"[RECAP] Daily recap FAILED to send.")

    # --- WEEKLY RECAP (Friday only) ---
    if now.weekday() == 4:  # Friday
        monday, friday = get_week_bounds(now)
        week_key = monday.strftime('%Y-%m-%d')
        last_weekly = stats.get('last_weekly_recap')
        if not last_weekly or not last_weekly.startswith(week_key):
            print(f"[RECAP] Generating weekly recap {week_key}...")
            msg = generate_weekly_recap(monday, friday)
            if send_telegram(msg):
                stats['last_weekly_recap'] = week_key + now.strftime(' %H:%M:%S')
                save_json(STATS_FILE, stats)
                print(f"[RECAP] Weekly recap sent.")
            else:
                print(f"[RECAP] Weekly recap FAILED to send.")

    # --- MONTHLY RECAP (last trading day of month) ---
    is_last_trading_day = False
    # Check if last weekday of month
    tomorrow = now + timedelta(days=1)
    if tomorrow.month != now.month and now.weekday() < 5:
        is_last_trading_day = True
    # Or last Friday before month ends
    elif now.weekday() == 4:
        next_friday = now + timedelta(days=7)
        if next_friday.month != now.month:
            # Check no weekday left after today in this month
            check = now + timedelta(days=1)
            has_weekday_left = False
            while check.month == now.month:
                if check.weekday() < 5:
                    has_weekday_left = True
                    break
                check += timedelta(days=1)
            if not has_weekday_left:
                is_last_trading_day = True

    if is_last_trading_day:
        month_key = now.strftime('%Y-%m')
        last_monthly = stats.get('last_monthly_recap')
        if not last_monthly or not last_monthly.startswith(month_key):
            print(f"[RECAP] Generating monthly recap {month_key}...")
            msg = generate_monthly_recap(now.year, now.month)
            if send_telegram(msg):
                stats['last_monthly_recap'] = month_key + now.strftime('-%d %H:%M:%S')
                save_json(STATS_FILE, stats)
                print(f"[RECAP] Monthly recap sent.")
            else:
                print(f"[RECAP] Monthly recap FAILED to send.")


def notify_signal(signal):
    """Send signal via all channels."""
    symbol = signal['symbol']
    direction = signal['direction']
    entry = signal['entry_price']
    sl = signal['sl_price']
    tp = signal['tp_price']
    rr = signal['rr']
    wick = signal['wick_pct']
    zone = signal['zone_size']

    print(f"\n  *** [{symbol}] {direction} SIGNAL ***")
    print(f"      Entry: {entry} | SL: {sl} | TP: {tp} | RR: 1:{rr}")
    print(f"      Wick: {wick}% | Zone: {zone} | HTF: {signal.get('htf_trend', '?')}")

    send_webhook(signal)

    # Telegram
    msg = (
        f"<b>{symbol} {direction}</b> @ {entry}\n"
        f"SL: {sl} | TP: {tp} | RR: 1:{rr}\n"
        f"Wick: {wick}% | Zone: {zone} | RSI: {signal.get('rsi', '?')}"
    )
    send_telegram(msg)


# ============================================================
# JSON DASHBOARD SERVER
# ============================================================

class DashboardHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET')
        self.send_header('Cache-Control', 'no-cache')
        super().end_headers()

    def do_GET(self):
        if self.path == '/api/status':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()

            active = load_json(ACTIVE_TRADES_FILE, [])
            history = load_json(TRADE_HISTORY_FILE, [])

            # Per-asset zone info
            zones_info = {}
            for sym in INSTRUMENTS:
                zone_state = load_json(STATE_DIR / f"zones_{sym.lower()}.json", {})
                zones_info[sym] = zone_state

            # Daily P&L
            today = datetime.utcnow().strftime('%Y-%m-%d')
            today_trades = [t for t in history if t.get('close_time', '').startswith(today)]
            today_pnl = sum(t.get('pnl_r', 0) for t in today_trades)

            # Bridge connection status
            try:
                br = requests.get(f"{BRIDGE_BASE_URL}/health", timeout=3)
                bridge_connected = br.status_code == 200
            except Exception:
                bridge_connected = False

            status_data = {
                'version': 'V10',
                'data_source': 'bridge_http',
                'bridge_connected': bridge_connected,
                'instruments': list(INSTRUMENTS.keys()),
                'stats': {
                    'total_signals': stats.get('total_signals', 0),
                    'trades_won': stats.get('trades_won', 0),
                    'trades_lost': stats.get('trades_lost', 0),
                    'total_r': round(stats.get('total_r', 0), 1),
                    'win_rate': round(
                        stats.get('trades_won', 0) / max(1, stats.get('trades_won', 0) + stats.get('trades_lost', 0)) * 100, 1
                    ),
                    'last_scan_time': stats.get('last_scan_time'),
                },
                'today': {
                    'trades': len(today_trades),
                    'pnl_r': round(today_pnl, 1),
                },
                'active_trades': active,
                'history': history[-30:],
                'zones': zones_info,
                'last_scan_per_asset': stats.get('last_scan_per_asset', {}),
                'config': {
                    'signal_lookback': SIGNAL_LOOKBACK,
                    'zone_max_candles': ZONE_MAX_CANDLES,
                    'portfolio_limits': PORTFOLIO_LIMITS,
                },
                'server_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            }
            self.wfile.write(json.dumps(status_data).encode())

        elif self.path.startswith('/api/asset/'):
            # Per-asset detail
            sym = self.path.split('/')[-1].upper()
            if sym in INSTRUMENTS:
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()

                active = [t for t in load_json(ACTIVE_TRADES_FILE, []) if t.get('symbol') == sym]
                history = [t for t in load_json(TRADE_HISTORY_FILE, []) if t.get('symbol') == sym]
                zones = load_json(STATE_DIR / f"zones_{sym.lower()}.json", {})
                params = ASSET_PARAMS.get(sym, {})

                data = {
                    'symbol': sym,
                    'instrument': INSTRUMENTS[sym],
                    'params': params,
                    'active_trades': active,
                    'history': history[-20:],
                    'zones': zones,
                    'last_scan': stats.get('last_scan_per_asset', {}).get(sym),
                }
                self.wfile.write(json.dumps(data, default=str).encode())
            else:
                self.send_error(404)
        else:
            self.send_error(404)

    def log_message(self, format, *args):
        pass  # Silence


def start_json_server():
    try:
        server = HTTPServer(('0.0.0.0', JSON_SERVER_PORT), DashboardHandler)
        print(f"[SERVER] Dashboard: http://localhost:{JSON_SERVER_PORT}/api/status")
        server.serve_forever()
    except Exception as e:
        print(f"[WARN] Server error: {e}")


# ============================================================
# SCHEDULING
# ============================================================

def is_weekend():
    now = datetime.utcnow()
    if now.weekday() == 5:
        return True
    if now.weekday() == 6 and now.hour < 22:
        return True
    if now.weekday() == 4 and now.hour >= 22:
        return True
    return False


def is_forex_trading_time():
    """Check if forex markets are open."""
    return not is_weekend()


def get_active_instruments():
    """Return instruments that should be scanned now."""
    active = []
    for sym, inst in INSTRUMENTS.items():
        if inst['source'] == 'binance':
            # Crypto: always active
            active.append(sym)
        elif is_forex_trading_time():
            active.append(sym)
    return active


# ============================================================
# MAIN SCANNER
# ============================================================

def scan_asset(symbol):
    """Scan a single asset for signals."""
    inst = INSTRUMENTS[symbol]
    n_candles = inst.get('candles_fetch', 1000)

    df_1m = fetch_data(symbol, n_candles)
    if df_1m is None or len(df_1m) < 100:
        return []

    # Build structure with V3 logic
    df_1m = build_structure_live(df_1m, symbol)

    # Check active trades
    closed = check_active_trades(symbol, df_1m)
    for ct in closed:
        send_result_webhook(ct)
        # Telegram result
        emoji = '+' if ct['status'] == 'TP' else '-'
        msg = f"{emoji} <b>{ct['symbol']} {ct['status']}</b>: {ct['direction']} | {ct['pnl_r']:+.1f}R | {ct['duration_minutes']}min"
        send_telegram(msg)

    # Detect new signals
    signals = detect_signals_live(df_1m, symbol)

    for signal in signals:
        # Portfolio risk check
        ok, reason = check_portfolio_risk(signal)
        if not ok:
            print(f"  [{symbol}] Signal blocked: {reason}")
            continue

        notify_signal(signal)
        add_active_trade(signal)

        global stats
        stats['total_signals'] = stats.get('total_signals', 0) + 1
        stats['signals_per_asset'] = stats.get('signals_per_asset', {})
        stats['signals_per_asset'][symbol] = stats['signals_per_asset'].get(symbol, 0) + 1
        save_json(STATS_FILE, stats)

    return signals


def scan_once():
    """Scan all active instruments once."""
    active_instruments = get_active_instruments()

    print(f"\n[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC] Scanning {len(active_instruments)} instruments...")

    all_signals = []

    for symbol in active_instruments:
        try:
            signals = scan_asset(symbol)
            n_zones = load_json(STATE_DIR / f"zones_{symbol.lower()}.json", {}).get('active_zones', 0)
            active_trades = [t for t in load_json(ACTIVE_TRADES_FILE, []) if t.get('symbol') == symbol]

            sig_count = len(signals)
            status = f"{sig_count} signal{'s' if sig_count != 1 else ''}" if sig_count > 0 else "no signal"
            print(f"  [{symbol}] {status} | {n_zones} zones | {len(active_trades)} trade(s)")

            all_signals.extend(signals)

        except Exception as e:
            print(f"  [{symbol}] ERROR: {e}")

    # Summary
    active_trades = load_json(ACTIVE_TRADES_FILE, [])
    print(f"\n  Total active trades: {len(active_trades)}")
    for t in active_trades:
        print(f"    - [{t['symbol']}] {t['direction']} @ {t['entry_price']} | SL: {t['sl_price']} TP: {t['tp_price']}")

    if not all_signals:
        print(f"  No new signals across all instruments")

    return all_signals


def run_continuous():
    """Run scanner continuously with staggered scheduling."""
    print(f"\n{'='*60}")
    print(f"HVC LIVE SCANNER V10 - BRIDGE HTTP")
    print(f"{'='*60}")
    print(f"Data source: MT5 via Bridge HTTP (real-time, zero IPC conflicts)")
    print(f"Instruments: {len(INSTRUMENTS)} ({sum(1 for i in INSTRUMENTS.values() if i['source']=='mt5')} MT5 + {sum(1 for i in INSTRUMENTS.values() if i['source']=='binance')} Binance)")
    print(f"Signal lookback: {SIGNAL_LOOKBACK} candles")
    print(f"Bridge: {BRIDGE_BASE_URL}")
    print(f"Zone persistence: {ZONE_MAX_CANDLES * 15}min max")
    print(f"Portfolio limits: max {PORTFOLIO_LIMITS['max_total_concurrent_trades']} concurrent, max {PORTFOLIO_LIMITS['max_correlated_trades']} correlated")
    print("=" * 60)

    # Show loaded configs
    for sym in INSTRUMENTS:
        params = ASSET_PARAMS.get(sym, {})
        sl = params.get('sl', '?')
        rr = params.get('rr', '?')
        src = INSTRUMENTS[sym]['source']
        interval = INSTRUMENTS[sym]['scan_interval']
        short_h = params.get('short_hours', [])
        long_h = params.get('long_hours', [])
        print(f"  {sym:8s} [{src:11s}] SL={sl} RR={rr} | SHORT:{short_h} LONG:{long_h} | every {interval}s")

    print(f"\nCtrl+C to stop\n")

    # Track last scan time per asset
    last_scanned = {}

    try:
        while True:
            now = datetime.utcnow()

            # Weekend check for forex
            if is_weekend():
                # Still scan crypto
                crypto = [s for s in INSTRUMENTS if INSTRUMENTS[s]['source'] == 'binance']
                if crypto:
                    for sym in crypto:
                        last = last_scanned.get(sym)
                        interval = INSTRUMENTS[sym]['scan_interval']
                        if last is None or (now - last).total_seconds() >= interval:
                            try:
                                scan_asset(sym)
                                last_scanned[sym] = now
                            except Exception as e:
                                print(f"  [{sym}] ERROR: {e}")
                    time.sleep(30)
                else:
                    print(f"\n[WEEKEND] All forex. Sleeping...")
                    time.sleep(300)
                continue

            # Scan each instrument based on its interval
            scanned_any = False
            for sym, inst in INSTRUMENTS.items():
                last = last_scanned.get(sym)
                interval = inst['scan_interval']

                if last is None or (now - last).total_seconds() >= interval:
                    try:
                        signals = scan_asset(sym)
                        last_scanned[sym] = datetime.utcnow()
                        scanned_any = True

                        if signals:
                            n_active = len(load_json(ACTIVE_TRADES_FILE, []))
                            print(f"  Active trades total: {n_active}")

                    except Exception as e:
                        print(f"  [{sym}] ERROR: {e}")

            if not scanned_any:
                # Nothing needed scanning, wait a bit
                time.sleep(10)
            else:
                # Short pause between scan cycles
                time.sleep(5)

            # Check for scheduled recaps (daily/weekly/monthly)
            check_and_send_recaps()

    except KeyboardInterrupt:
        print("\n\nScanner V10 stopped.")
        # Save final state
        save_json(STATS_FILE, stats)


# ============================================================
# CLI COMMANDS
# ============================================================

def show_status():
    print("=" * 70)
    print("HVC SCANNER V10 - STATUS")
    print("=" * 70)

    total_trades = stats.get('trades_won', 0) + stats.get('trades_lost', 0)
    wr = (stats.get('trades_won', 0) / total_trades * 100) if total_trades > 0 else 0

    print(f"\nData source: MT5")
    print(f"Last scan: {stats.get('last_scan_time', 'Never')}")
    print(f"Total signals: {stats.get('total_signals', 0)}")
    print(f"Performance: {stats.get('trades_won', 0)}W / {stats.get('trades_lost', 0)}L | WR: {wr:.1f}% | {stats.get('total_r', 0):+.1f}R")

    print(f"\nLast scan per asset:")
    for sym in INSTRUMENTS:
        last = stats.get('last_scan_per_asset', {}).get(sym, 'Never')
        zone_info = load_json(STATE_DIR / f"zones_{sym.lower()}.json", {})
        n_zones = zone_info.get('active_zones', 0)
        n_signals = stats.get('signals_per_asset', {}).get(sym, 0)
        print(f"  {sym:8s} | Last: {last} | Zones: {n_zones} | Signals: {n_signals}")

    active = load_json(ACTIVE_TRADES_FILE, [])
    print(f"\nActive trades: {len(active)}")
    for t in active:
        print(f"  [{t['symbol']}] {t['direction']} @ {t['entry_price']} | SL: {t['sl_price']} TP: {t['tp_price']}")

    print(f"\nMarket: {'WEEKEND' if is_weekend() else 'OPEN'}")
    print(f"Time: {datetime.utcnow().strftime('%H:%M')} UTC")


def show_performance():
    print("=" * 70)
    print("HVC SCANNER V10 - PERFORMANCE")
    print("=" * 70)

    history = load_json(TRADE_HISTORY_FILE, [])
    if not history:
        print("\nNo trades yet.")
        return

    # Overall
    won = sum(1 for t in history if t.get('status') == 'TP')
    lost = sum(1 for t in history if t.get('status') == 'SL')
    total = won + lost
    wr = won / total * 100 if total > 0 else 0
    total_r = sum(t.get('pnl_r', 0) for t in history)

    print(f"\nOverall: {total} trades | WR: {wr:.1f}% | {total_r:+.1f}R")

    # Per asset
    print(f"\nPer asset:")
    assets = set(t.get('symbol', '?') for t in history)
    for sym in sorted(assets):
        sym_trades = [t for t in history if t.get('symbol') == sym]
        sym_won = sum(1 for t in sym_trades if t.get('status') == 'TP')
        sym_lost = sum(1 for t in sym_trades if t.get('status') == 'SL')
        sym_total = sym_won + sym_lost
        sym_wr = sym_won / sym_total * 100 if sym_total > 0 else 0
        sym_r = sum(t.get('pnl_r', 0) for t in sym_trades)
        print(f"  {sym:8s} | {sym_total:3d} trades | WR: {sym_wr:5.1f}% | {sym_r:+6.1f}R")

    # Last 10
    print(f"\nLast 10 trades:")
    for t in history[-10:]:
        emoji = '+' if t.get('status') == 'TP' else '-'
        print(f"  {emoji} [{t.get('symbol', '?'):8s}] {t.get('direction', '?')} {t.get('status', '?')} | {t.get('pnl_r', 0):+.1f}R | {t.get('duration_minutes', '?')}min")


def run_serve():
    server_thread = threading.Thread(target=start_json_server, daemon=True)
    server_thread.start()
    run_continuous()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    if "--status" in sys.argv:
        show_status()
    elif "--performance" in sys.argv:
        show_performance()
    else:
        # Bridge connection required for scanning (bridge owns the MT5 connection)
        if not bridge_check():
            print("[FATAL] Cannot start without bridge connection. Is hvc_mt5_bridge.py running?")
            sys.exit(1)

        if "--once" in sys.argv:
            scan_once()
        elif "--serve" in sys.argv:
            run_serve()
        else:
            run_continuous()

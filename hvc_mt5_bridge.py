#!/usr/bin/env python3
"""
HVC MT5 Bridge - Auto-Trading via Webhook
Scanner V9 (12 instruments) → FastAPI → MetaTrader 5 → Startrader

Usage:
  python hvc_mt5_bridge.py                    # Normal mode
  python hvc_mt5_bridge.py --dry-run          # Simulation (no real trades)
  python hvc_mt5_bridge.py --discover-symbols # List all MT5 symbols then exit
"""

import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

import MetaTrader5 as mt5
import uvicorn
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

# ════════════════════════════════════════════
# PATHS
# ════════════════════════════════════════════

BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.json"
STATE_DIR = BASE_DIR / "state"
LOG_DIR = BASE_DIR / "logs"

STATE_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

TRADE_LOG_PATH = STATE_DIR / "trade_log.json"
DAILY_STATS_PATH = STATE_DIR / "daily_stats.json"

# ════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════

logger = logging.getLogger("hvc-mt5-bridge")
logger.setLevel(logging.INFO)

console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(console)

file_handler = RotatingFileHandler(
    LOG_DIR / "bridge.log", maxBytes=5_000_000, backupCount=5
)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
)
logger.addHandler(file_handler)

# ════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════

def load_config() -> dict:
    if not CONFIG_PATH.exists():
        logger.error(f"Config not found: {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH) as f:
        return json.load(f)

config = load_config()

# ════════════════════════════════════════════
# GLOBALS
# ════════════════════════════════════════════

DRY_RUN = "--dry-run" in sys.argv
MAGIC = config["risk"]["magic_number"]
MAX_SPREAD = config["risk"].get("max_spread_points", 100)

# Daily stats (reset each day)
daily_stats = {
    "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    "signals_received": 0,
    "trades_executed": 0,
    "trades_rejected": 0,
    "errors": 0,
}

# Known positions (for monitoring)
known_positions: dict = {}

# Start time
start_time = time.time()

# ════════════════════════════════════════════
# MT5 CONNECTION
# ════════════════════════════════════════════

def mt5_connect() -> bool:
    """Connect to MT5 terminal and login."""
    mt5_cfg = config["mt5"]
    terminal_path = mt5_cfg.get("terminal_path")

    init_kwargs = {}
    if terminal_path:
        init_kwargs["path"] = terminal_path

    if not mt5.initialize(**init_kwargs):
        logger.error(f"MT5 initialize failed: {mt5.last_error()}")
        return False

    authorized = mt5.login(
        login=mt5_cfg["account"],
        password=mt5_cfg["password"],
        server=mt5_cfg["server"],
    )
    if not authorized:
        logger.error(f"MT5 login failed: {mt5.last_error()}")
        mt5.shutdown()
        return False

    acct = mt5.account_info()
    logger.info(
        f"MT5 connected - Account: {acct.login} | "
        f"Balance: ${acct.balance:.2f} | Equity: ${acct.equity:.2f} | "
        f"Leverage: 1:{acct.leverage}"
    )
    return True


def ensure_connected() -> bool:
    """Check MT5 connection, reconnect if needed."""
    try:
        info = mt5.terminal_info()
        if info is not None:
            return True
    except Exception:
        pass

    logger.warning("MT5 disconnected, reconnecting...")
    try:
        mt5.shutdown()
    except Exception:
        pass
    time.sleep(1)
    return mt5_connect()


def discover_symbols():
    """Print all symbols available in MT5 then exit."""
    if not mt5_connect():
        print("Failed to connect to MT5")
        sys.exit(1)

    symbols = mt5.symbols_get()
    if not symbols:
        print("No symbols found")
        mt5.shutdown()
        sys.exit(1)

    scanner_symbols = list(config.get("symbol_map", {}).keys())
    print(f"\n{'='*60}")
    print(f"MT5 SYMBOLS ({len(symbols)} total)")
    print(f"{'='*60}\n")

    # Show scanner-relevant symbols first
    print("SCANNER MATCHES:")
    for sym in sorted(symbols, key=lambda s: s.name):
        base = sym.name.replace(".", "").replace("sv", "").replace("m", "").upper()
        for scanner_sym in scanner_symbols:
            if scanner_sym in sym.name.upper() or scanner_sym == base:
                print(
                    f"  {sym.name:<20} spread={sym.spread:>4} "
                    f"tick_value={sym.trade_tick_value:.4f} "
                    f"tick_size={sym.trade_tick_size} "
                    f"vol_min={sym.volume_min} vol_step={sym.volume_step}"
                )
                break

    print(f"\nALL SYMBOLS:")
    for sym in sorted(symbols, key=lambda s: s.name):
        print(f"  {sym.name}")

    mt5.shutdown()
    sys.exit(0)


# ════════════════════════════════════════════
# SYMBOL MAPPING
# ════════════════════════════════════════════

def get_mt5_symbol(scanner_symbol: str) -> Optional[str]:
    """Map scanner symbol to broker symbol. Returns None if disabled."""
    mapped = config.get("symbol_map", {}).get(scanner_symbol)
    return mapped  # None means skip


# ════════════════════════════════════════════
# LOT SIZE CALCULATION
# ════════════════════════════════════════════

def calculate_lot_size(
    mt5_symbol: str, entry_price: float, sl_price: float, risk_pct: float
) -> float:
    """
    Dynamic lot sizing using MT5 symbol info.
    Works for any instrument (forex, gold, crypto).
    """
    acct = mt5.account_info()
    if not acct:
        return 0.0

    risk_amount = acct.equity * (risk_pct / 100)

    info = mt5.symbol_info(mt5_symbol)
    if not info:
        logger.error(f"Symbol info not available: {mt5_symbol}")
        return 0.0

    tick_value = info.trade_tick_value
    tick_size = info.trade_tick_size

    if tick_value <= 0 or tick_size <= 0:
        logger.error(f"Invalid tick data for {mt5_symbol}: tv={tick_value} ts={tick_size}")
        return 0.0

    sl_distance = abs(entry_price - sl_price)
    if sl_distance <= 0:
        logger.error(f"Invalid SL distance: entry={entry_price} sl={sl_price}")
        return 0.0

    ticks_in_sl = sl_distance / tick_size
    risk_per_lot = ticks_in_sl * tick_value

    if risk_per_lot <= 0:
        return 0.0

    lot = risk_amount / risk_per_lot

    # Apply max lot from config
    max_lot_config = config["risk"].get("max_lot_per_trade", 10.0)
    lot = min(lot, max_lot_config)

    # Round to broker step
    vol_step = info.volume_step
    lot = round(lot / vol_step) * vol_step
    lot = max(info.volume_min, min(lot, info.volume_max))

    return round(lot, 2)


# ════════════════════════════════════════════
# FILLING TYPE
# ════════════════════════════════════════════

def get_filling_type(mt5_symbol: str) -> int:
    """Auto-detect filling type supported by broker for this symbol."""
    info = mt5.symbol_info(mt5_symbol)
    if not info:
        return mt5.ORDER_FILLING_IOC

    filling = info.filling_mode
    # Use numeric constants for compatibility (FOK=1, IOC=2)
    FILLING_FOK = getattr(mt5, 'SYMBOL_FILLING_FOK', 1)
    FILLING_IOC = getattr(mt5, 'SYMBOL_FILLING_IOC', 2)
    if filling & FILLING_FOK:
        return mt5.ORDER_FILLING_FOK
    elif filling & FILLING_IOC:
        return mt5.ORDER_FILLING_IOC
    return mt5.ORDER_FILLING_RETURN


# ════════════════════════════════════════════
# TRADE EXECUTION
# ════════════════════════════════════════════

def execute_signal(signal: dict) -> dict:
    """Execute a trading signal on MT5. Returns result dict."""
    symbol = signal.get("symbol", "")
    direction = signal.get("direction", "")
    entry_price = float(signal.get("entry_price", 0))
    sl_price = float(signal.get("sl_price", 0))
    tp_price = float(signal.get("tp_price", 0))

    # Map symbol
    mt5_symbol = get_mt5_symbol(symbol)
    if not mt5_symbol:
        return {"success": False, "error": f"Symbol {symbol} not mapped or disabled"}

    # Ensure connected
    if not ensure_connected():
        return {"success": False, "error": "MT5 not connected"}

    # Select symbol
    if not mt5.symbol_select(mt5_symbol, True):
        return {"success": False, "error": f"Cannot select {mt5_symbol}"}

    # Check spread
    tick = mt5.symbol_info_tick(mt5_symbol)
    if not tick:
        return {"success": False, "error": f"No tick data for {mt5_symbol}"}

    info = mt5.symbol_info(mt5_symbol)
    spread_points = (tick.ask - tick.bid) / info.point if info.point > 0 else 0
    max_spread = config["risk"].get("max_spread_points", {})
    if isinstance(max_spread, dict):
        max_sp = max_spread.get(symbol, 100)
    else:
        max_sp = max_spread

    if spread_points > max_sp:
        return {
            "success": False,
            "error": f"Spread too high: {spread_points:.0f} points (max {max_sp})",
        }

    # Risk percentage (configurable per-symbol or default)
    risk_overrides = config["risk"].get("symbol_risk_pct", {})
    risk_pct = risk_overrides.get(symbol, config["risk"]["default_risk_pct"])

    # Calculate lot
    lot = calculate_lot_size(mt5_symbol, entry_price, sl_price, risk_pct)
    if lot <= 0:
        return {"success": False, "error": f"Lot size 0 for {mt5_symbol}"}

    # Order type
    if direction == "LONG":
        order_type = mt5.ORDER_TYPE_BUY
        price = tick.ask
    elif direction == "SHORT":
        order_type = mt5.ORDER_TYPE_SELL
        price = tick.bid
    else:
        return {"success": False, "error": f"Invalid direction: {direction}"}

    # DRY RUN
    if DRY_RUN:
        logger.info(
            f"[DRY-RUN] Would execute {direction} {mt5_symbol} "
            f"lot={lot} price={price:.5f} sl={sl_price:.5f} tp={tp_price:.5f}"
        )
        return {
            "success": True,
            "dry_run": True,
            "symbol": mt5_symbol,
            "direction": direction,
            "lot": lot,
            "price": price,
            "sl": sl_price,
            "tp": tp_price,
        }

    # Build order request
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": mt5_symbol,
        "volume": lot,
        "type": order_type,
        "price": price,
        "sl": round(sl_price, info.digits),
        "tp": round(tp_price, info.digits),
        "deviation": config["risk"].get("slippage_points", 30),
        "magic": MAGIC,
        "comment": f"HVC_{symbol}_{direction}",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": get_filling_type(mt5_symbol),
    }

    # Execute with retry (handles requotes)
    max_retries = 3
    for attempt in range(max_retries):
        result = mt5.order_send(request)

        if result is None:
            error = mt5.last_error()
            return {"success": False, "error": f"order_send None: {error}"}

        if result.retcode == mt5.TRADE_RETCODE_DONE:
            trade_result = {
                "success": True,
                "ticket": result.order,
                "symbol": mt5_symbol,
                "direction": direction,
                "lot": result.volume,
                "price": result.price,
                "sl": sl_price,
                "tp": tp_price,
                "spread_points": spread_points,
                "risk_pct": risk_pct,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            logger.info(
                f"EXECUTED: {direction} {mt5_symbol} | "
                f"Ticket: {result.order} | Lot: {result.volume} | "
                f"Price: {result.price:.5f} | Spread: {spread_points:.0f}pt"
            )

            # Log to file
            _log_trade(trade_result)
            return trade_result

        elif result.retcode == 10004:  # Requote
            logger.warning(f"Requote on attempt {attempt + 1}, retrying...")
            time.sleep(0.2)
            tick = mt5.symbol_info_tick(mt5_symbol)
            if tick:
                request["price"] = tick.ask if direction == "LONG" else tick.bid
            continue
        else:
            return {
                "success": False,
                "error": f"retcode={result.retcode}: {result.comment}",
            }

    return {"success": False, "error": "Max retries exceeded (requotes)"}


def close_position(ticket: int) -> dict:
    """Close a position by ticket number."""
    if DRY_RUN:
        return {"success": True, "dry_run": True, "ticket": ticket}

    if not ensure_connected():
        return {"success": False, "error": "MT5 not connected"}

    positions = mt5.positions_get(ticket=ticket)
    if not positions:
        return {"success": False, "error": f"Position {ticket} not found"}

    pos = positions[0]
    symbol = pos.symbol
    tick = mt5.symbol_info_tick(symbol)
    if not tick:
        return {"success": False, "error": f"No tick for {symbol}"}

    if pos.type == mt5.ORDER_TYPE_BUY:
        order_type = mt5.ORDER_TYPE_SELL
        price = tick.bid
    else:
        order_type = mt5.ORDER_TYPE_BUY
        price = tick.ask

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": pos.volume,
        "type": order_type,
        "position": ticket,
        "price": price,
        "deviation": 30,
        "magic": MAGIC,
        "comment": "HVC_CLOSE",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": get_filling_type(symbol),
    }

    result = mt5.order_send(request)
    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
        logger.info(f"CLOSED: Ticket {ticket} @ {result.price}")
        return {"success": True, "ticket": ticket, "close_price": result.price}

    error = result.comment if result else str(mt5.last_error())
    return {"success": False, "error": f"Close failed: {error}"}


def close_all_positions() -> dict:
    """Close all HVC positions (filtered by magic number)."""
    if not ensure_connected():
        return {"success": False, "error": "MT5 not connected"}

    positions = mt5.positions_get()
    if not positions:
        return {"success": True, "closed": 0, "message": "No positions"}

    closed = 0
    errors = []
    for pos in positions:
        if pos.magic != MAGIC:
            continue
        result = close_position(pos.ticket)
        if result["success"]:
            closed += 1
        else:
            errors.append(f"Ticket {pos.ticket}: {result.get('error')}")

    return {"success": len(errors) == 0, "closed": closed, "errors": errors}


# ════════════════════════════════════════════
# TRADE LOGGING
# ════════════════════════════════════════════

def _log_trade(trade: dict):
    """Append trade to trade_log.json."""
    try:
        trades = []
        if TRADE_LOG_PATH.exists():
            with open(TRADE_LOG_PATH) as f:
                trades = json.load(f)
        trades.append(trade)
        # Keep last 500 trades
        trades = trades[-500:]
        with open(TRADE_LOG_PATH, "w") as f:
            json.dump(trades, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to log trade: {e}")


def _update_daily_stats(field: str):
    """Increment a daily stats counter."""
    global daily_stats
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if daily_stats["date"] != today:
        daily_stats = {
            "date": today,
            "signals_received": 0,
            "trades_executed": 0,
            "trades_rejected": 0,
            "errors": 0,
        }
    daily_stats[field] = daily_stats.get(field, 0) + 1


# ════════════════════════════════════════════
# POSITION MONITOR (background thread)
# ════════════════════════════════════════════

def position_monitor_loop():
    """Background thread: detect closed positions."""
    global known_positions

    while True:
        try:
            if not ensure_connected():
                time.sleep(60)
                continue

            positions = mt5.positions_get()
            current_tickets = set()

            if positions:
                for pos in positions:
                    if pos.magic != MAGIC:
                        continue
                    current_tickets.add(pos.ticket)

                    if pos.ticket not in known_positions:
                        known_positions[pos.ticket] = {
                            "symbol": pos.symbol,
                            "type": "BUY" if pos.type == 0 else "SELL",
                            "volume": pos.volume,
                            "open_price": pos.price_open,
                            "sl": pos.sl,
                            "tp": pos.tp,
                            "open_time": datetime.fromtimestamp(
                                pos.time, tz=timezone.utc
                            ).isoformat(),
                        }

                    known_positions[pos.ticket]["profit"] = pos.profit

            # Detect closed
            closed_tickets = set(known_positions.keys()) - current_tickets
            for ticket in closed_tickets:
                pos_data = known_positions.pop(ticket)
                logger.info(
                    f"POSITION CLOSED: Ticket {ticket} | "
                    f"{pos_data['symbol']} {pos_data['type']} | "
                    f"Last P&L: ${pos_data.get('profit', 0):.2f}"
                )
                # Could send notification to n8n here
                _notify_closed_position(ticket, pos_data)

            time.sleep(30)

        except Exception as e:
            logger.error(f"Position monitor error: {e}")
            time.sleep(60)


def _notify_closed_position(ticket: int, pos_data: dict):
    """Send closed position notification via webhook (optional)."""
    webhook_url = config.get("notifications", {}).get("result_webhook")
    if not webhook_url:
        return

    try:
        import requests

        payload = {
            "type": "MT5_POSITION_CLOSED",
            "ticket": ticket,
            "symbol": pos_data["symbol"],
            "direction": pos_data["type"],
            "volume": pos_data["volume"],
            "open_price": pos_data["open_price"],
            "last_profit": pos_data.get("profit", 0),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        requests.post(webhook_url, json=payload, timeout=10)
    except Exception as e:
        logger.error(f"Failed to notify closed position: {e}")


# ════════════════════════════════════════════
# HEARTBEAT (background thread)
# ════════════════════════════════════════════

def heartbeat_loop():
    """Send heartbeat every 60s to n8n for monitoring."""
    heartbeat_url = config.get("notifications", {}).get("heartbeat_webhook")
    if not heartbeat_url:
        return  # No heartbeat configured, exit thread

    import requests

    while True:
        try:
            connected = mt5.terminal_info() is not None
            acct = mt5.account_info()

            payload = {
                "status": "alive",
                "dry_run": DRY_RUN,
                "mt5_connected": connected,
                "equity": acct.equity if acct else 0,
                "balance": acct.balance if acct else 0,
                "positions": len(
                    [p for p in (mt5.positions_get() or []) if p.magic == MAGIC]
                ),
                "uptime_seconds": int(time.time() - start_time),
                "daily_stats": daily_stats,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            requests.post(heartbeat_url, json=payload, timeout=5)
        except Exception:
            pass  # Heartbeat failures are non-critical

        time.sleep(60)


# ════════════════════════════════════════════
# FASTAPI APP
# ════════════════════════════════════════════

app = FastAPI(title="HVC MT5 Bridge", version="1.0")


def verify_api_key(x_api_key: str = Header(None)):
    """Validate API key from request header."""
    expected = config.get("security", {}).get("api_key", "")
    if not expected:
        return  # No API key configured, allow all
    if x_api_key != expected:
        raise HTTPException(status_code=403, detail="Invalid API key")


class SignalPayload(BaseModel):
    symbol: str
    direction: str
    entry_price: float
    sl_price: float
    tp_price: float
    sl_amount: Optional[float] = None
    tp_amount: Optional[float] = None
    rr: Optional[float] = None
    wick_pct: Optional[float] = None
    rsi: Optional[float] = None
    htf_trend: Optional[str] = None
    timestamp_utc: Optional[str] = None
    source: Optional[str] = None


@app.post("/signal")
async def receive_signal(
    signal: SignalPayload, x_api_key: str = Header(None)
):
    verify_api_key(x_api_key)
    _update_daily_stats("signals_received")

    logger.info(
        f"SIGNAL: {signal.direction} {signal.symbol} | "
        f"Entry: {signal.entry_price} SL: {signal.sl_price} TP: {signal.tp_price}"
    )

    result = execute_signal(signal.model_dump())

    if result.get("success"):
        _update_daily_stats("trades_executed")
    else:
        _update_daily_stats("trades_rejected")
        logger.warning(f"REJECTED: {signal.symbol} - {result.get('error')}")

    return result


@app.get("/status")
async def get_status():
    connected = False
    account_data = {}

    try:
        info = mt5.terminal_info()
        connected = info is not None
        acct = mt5.account_info()
        if acct:
            account_data = {
                "login": acct.login,
                "server": acct.server,
                "balance": acct.balance,
                "equity": acct.equity,
                "margin_free": acct.margin_free,
                "leverage": acct.leverage,
            }
    except Exception:
        pass

    # Open positions
    open_positions = []
    try:
        positions = mt5.positions_get()
        if positions:
            for pos in positions:
                if pos.magic != MAGIC:
                    continue
                open_positions.append(
                    {
                        "ticket": pos.ticket,
                        "symbol": pos.symbol,
                        "type": "BUY" if pos.type == 0 else "SELL",
                        "volume": pos.volume,
                        "open_price": pos.price_open,
                        "sl": pos.sl,
                        "tp": pos.tp,
                        "profit": pos.profit,
                        "open_time": datetime.fromtimestamp(
                            pos.time, tz=timezone.utc
                        ).isoformat(),
                    }
                )
    except Exception:
        pass

    return {
        "bridge_version": "1.0",
        "dry_run": DRY_RUN,
        "uptime_seconds": int(time.time() - start_time),
        "mt5_connected": connected,
        "account": account_data,
        "today": daily_stats,
        "open_positions": open_positions,
        "open_count": len(open_positions),
        "unrealized_pnl": sum(p["profit"] for p in open_positions),
        "risk_config": {
            "default_risk_pct": config["risk"]["default_risk_pct"],
            "max_lot": config["risk"].get("max_lot_per_trade", 10),
            "magic": MAGIC,
        },
        "symbol_map": config.get("symbol_map", {}),
        "server_time": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/positions")
async def get_positions():
    if not ensure_connected():
        raise HTTPException(status_code=503, detail="MT5 not connected")

    positions = mt5.positions_get()
    result = []
    if positions:
        for pos in positions:
            if pos.magic != MAGIC:
                continue
            result.append(
                {
                    "ticket": pos.ticket,
                    "symbol": pos.symbol,
                    "type": "BUY" if pos.type == 0 else "SELL",
                    "volume": pos.volume,
                    "open_price": pos.price_open,
                    "current_price": pos.price_current,
                    "sl": pos.sl,
                    "tp": pos.tp,
                    "profit": pos.profit,
                    "swap": pos.swap,
                    "open_time": datetime.fromtimestamp(
                        pos.time, tz=timezone.utc
                    ).isoformat(),
                }
            )
    return {"positions": result, "count": len(result)}


@app.post("/close/{ticket}")
async def api_close_position(ticket: int, x_api_key: str = Header(None)):
    verify_api_key(x_api_key)
    return close_position(ticket)


@app.post("/close-all")
async def api_close_all(x_api_key: str = Header(None)):
    verify_api_key(x_api_key)
    return close_all_positions()


@app.get("/health")
async def health():
    return {"status": "ok", "dry_run": DRY_RUN}


# ════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════

def main():
    if "--discover-symbols" in sys.argv:
        discover_symbols()
        return

    mode = "DRY-RUN" if DRY_RUN else "LIVE"
    logger.info(f"{'='*60}")
    logger.info(f"HVC MT5 BRIDGE - {mode} MODE")
    logger.info(f"{'='*60}")
    logger.info(f"Magic: {MAGIC}")
    logger.info(f"Risk: {config['risk']['default_risk_pct']}%")
    logger.info(f"Symbols: {sum(1 for v in config.get('symbol_map', {}).values() if v)} mapped")

    # Connect MT5
    if not DRY_RUN:
        if not mt5_connect():
            logger.error("Failed to connect to MT5. Exiting.")
            sys.exit(1)

    # Start background threads
    monitor_thread = threading.Thread(target=position_monitor_loop, daemon=True)
    monitor_thread.start()

    heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()

    logger.info("Background monitors started")

    # Start FastAPI
    host = config.get("server", {}).get("host", "0.0.0.0")
    port = config.get("server", {}).get("port", 8080)
    logger.info(f"Starting API on {host}:{port}")

    uvicorn.run(app, host=host, port=port, log_level="warning")


if __name__ == "__main__":
    main()

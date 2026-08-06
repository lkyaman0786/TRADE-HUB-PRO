import sys
import subprocess

# Self-bootstrapping packages check and installation
REQUIRED_PACKAGES = {
    "pyotp": "pyotp",
    "SmartApi": "smartapi-python",
    "fyers_apiv3": "fyers-apiv3",
    "kiteconnect": "kiteconnect",
    "flask": "Flask",
    "requests": "requests",
    "setuptools": "setuptools"
}

def bootstrap_packages():
    missing_packages = []
    for import_name, install_name in REQUIRED_PACKAGES.items():
        try:
            __import__(import_name)
        except ImportError:
            missing_packages.append(install_name)
            
    if missing_packages:
        print(f"[BOOTSTRAP] Missing packages detected: {missing_packages}")
        print("[BOOTSTRAP] Attempting to install missing packages...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", *missing_packages])
            print("[BOOTSTRAP] All missing packages installed successfully!\n")
        except Exception as e:
            print(f"[BOOTSTRAP] [WARNING] Could not auto-install via pip: {e}. Continuing with available modules.")

bootstrap_packages()

import os
import time
import json
import re
import datetime
from datetime import datetime, date
import threading
import webbrowser
import sqlite3
import hashlib
from flask import Flask, jsonify, request, send_from_directory, send_file, session, redirect, url_for, render_template
import requests

try:
    import pyotp
    PYOTP_AVAILABLE = True
except ImportError:
    PYOTP_AVAILABLE = False
    print("[WARNING] pyotp module not available.")



# Monkey patch requests to prevent any infinite hangs in external libraries
_original_send = requests.Session.send
def _patched_send(self, request, **kwargs):
    if 'timeout' not in kwargs or kwargs['timeout'] is None:
        kwargs['timeout'] = 15.0
    return _original_send(self, request, **kwargs)
requests.Session.send = _patched_send

# Import SmartApi from AngelOne
try:
    from SmartApi import SmartConnect
    ANGEL_AVAILABLE = True
except ImportError:
    ANGEL_AVAILABLE = False
    print("[WARNING] SmartApi (smartapi-python) not installed. Angel One broker will not be available.")

# Import Fyers API v3 (optional - only required if user selects Fyers broker)
try:
    from fyers_apiv3 import fyersModel as FyersModel
    FYERS_AVAILABLE = True
except ImportError:
    FYERS_AVAILABLE = False
    print("[WARNING] fyers-apiv3 not installed. Fyers broker will not be available.")

# Import KiteConnect for Zerodha (optional)
try:
    from kiteconnect import KiteConnect, KiteTicker
    ZERODHA_AVAILABLE = True
except ImportError:
    ZERODHA_AVAILABLE = False
    print("[WARNING] kiteconnect not installed. Zerodha broker will not be available.")

import urllib.parse
import csv
import io

# ==========================================
# 1. CORE CONFIGURATION & PERSISTENCE
# ==========================================

# Thread safety locks
state_lock = threading.RLock()


# App State Globals (Migrated to Multi-Tenant)
import contextvars
client_ctx = contextvars.ContextVar('client_ctx', default='DEFAULT')

CLIENT_DATA_DIR = "client_data"

def get_client_file_path(filename):
    if not os.path.exists(CLIENT_DATA_DIR):
        os.makedirs(CLIENT_DATA_DIR, exist_ok=True)
    return os.path.join(CLIENT_DATA_DIR, os.path.basename(filename))

def migrate_client_data_files():
    if not os.path.exists(CLIENT_DATA_DIR):
        os.makedirs(CLIENT_DATA_DIR, exist_ok=True)
    import glob, shutil
    patterns = ["client_config_*.json", "client_config.json", "strategies_*.json", "strategies.json", "tokens_*.json", "tokens.json"]
    for pattern in patterns:
        for fpath in glob.glob(pattern):
            if fpath.startswith("client_data") or "example" in fpath:
                continue
            if os.path.isfile(fpath):
                dest = os.path.join(CLIENT_DATA_DIR, os.path.basename(fpath))
                try:
                    if not os.path.exists(dest):
                        shutil.move(fpath, dest)
                        print(f"[INFO] Migrated client data file '{fpath}' -> '{dest}'")
                except Exception as e:
                    print(f"[WARNING] Could not move {fpath}: {e}")

class MultiTenantState:
    def __init__(self):
        self.sessions = {}
        self.engine_running = False

    def get_session(self, cc=None):
        if cc is None:
            cc = client_ctx.get()
        if cc not in self.sessions:
            broker_client = UnifiedBrokerClient()
            self.sessions[cc] = {
                'state.active_strategies': {},
                'state.app_logs': [],
                'state.unified_broker': broker_client
            }
            # Auto-load saved strategies for this tenant
            target_strat = get_client_file_path(f"strategies_{cc}.json" if cc != 'DEFAULT' else "strategies.json")
            if not os.path.exists(target_strat):
                root_strat = f"strategies_{cc}.json" if cc != 'DEFAULT' else "strategies.json"
                if os.path.exists(root_strat):
                    target_strat = root_strat
                elif os.path.exists(get_client_file_path("strategies.json")):
                    target_strat = get_client_file_path("strategies.json")
                elif os.path.exists("strategies.json"):
                    target_strat = "strategies.json"
            if os.path.exists(target_strat):
                try:
                    with open(target_strat, "r") as f:
                        self.sessions[cc]['state.active_strategies'] = json.load(f)
                except Exception as e:
                    print(f"[ERROR] Failed loading strategies for tenant {cc}: {e}")
                    
            # Auto-connect broker from saved config for this tenant
            target_cfg = get_client_file_path(f"client_config_{cc}.json" if cc != 'DEFAULT' else "client_config.json")
            if not os.path.exists(target_cfg):
                root_cfg = f"client_config_{cc}.json" if cc != 'DEFAULT' else "client_config.json"
                if os.path.exists(root_cfg):
                    target_cfg = root_cfg
                elif os.path.exists(get_client_file_path("client_config.json")):
                    target_cfg = get_client_file_path("client_config.json")
                elif os.path.exists("client_config.json"):
                    target_cfg = "client_config.json"
            if os.path.exists(target_cfg):
                try:
                    with open(target_cfg, "r") as f:
                        cfg_data = json.load(f)
                    broker_client.connect(cfg_data)
                except Exception as e:
                    print(f"[ERROR] Failed auto-connecting broker for tenant {cc}: {e}")
        return self.sessions[cc]

    @property
    def active_strategies(self):
        return self.get_session()['state.active_strategies']
        
    @active_strategies.setter
    def active_strategies(self, val):
        self.get_session()['state.active_strategies'] = val

    @property
    def app_logs(self):
        return self.get_session()['state.app_logs']

    @property
    def unified_broker(self):
        return self.get_session()['state.unified_broker']
        
    @unified_broker.setter
    def unified_broker(self, val):
        self.get_session()['state.unified_broker'] = val

    @property
    def STRATEGIES_FILE(self):
        cc = client_ctx.get()
        return get_client_file_path(f"strategies_{cc}.json" if cc != 'DEFAULT' else "strategies.json")
        
    @property
    def TOKENS_FILE(self):
        cc = client_ctx.get()
        return get_client_file_path(f"tokens_{cc}.json" if cc != 'DEFAULT' else "tokens.json")

    @property
    def CLIENT_CONFIG_FILE(self):
        cc = client_ctx.get()
        return get_client_file_path(f"client_config_{cc}.json" if cc != 'DEFAULT' else "client_config.json")

state = MultiTenantState()

lookup_engine = None

import random
last_nifty_price = 24194.65
nifty_prev_close = 24021.65
last_nifty_fetch_time = 0.0

def get_nifty_live_price():
    global last_nifty_price, nifty_prev_close, last_nifty_fetch_time
    now = time.time()
    if now - last_nifty_fetch_time > 0.25:
        last_nifty_fetch_time = now
        if state.unified_broker and state.unified_broker.connected:
            try:
                if state.unified_broker.broker == "ANGEL_ONE":
                    res = state.unified_broker.get_market_data({"NSE": ["99926000"]})
                    if res and "99926000" in res and res["99926000"].get("ltp"):
                        last_nifty_price = float(res["99926000"]["ltp"])
                        if res["99926000"].get("close", 0) > 0:
                            nifty_prev_close = float(res["99926000"]["close"])
                elif state.unified_broker.broker == "ZERODHA":
                    res = state.unified_broker.get_market_data({"NSE": ["NSE:NIFTY 50"]})
                    if res:
                        for k, v in res.items():
                            if v.get("ltp"):
                                last_nifty_price = float(v["ltp"])
                elif state.unified_broker.broker == "FYERS":
                    res = state.unified_broker.get_market_data({"NSE": ["NSE:NIFTY50-INDEX"]})
                    if res:
                        for k, v in res.items():
                            if v.get("ltp"):
                                last_nifty_price = float(v["ltp"])
            except Exception:
                pass

    if nifty_prev_close <= 0:
        nifty_prev_close = 24021.65

    change_val = round(last_nifty_price - nifty_prev_close, 2)
    change_pct = round((change_val / nifty_prev_close) * 100, 2)
    return round(last_nifty_price, 2), change_val, change_pct

def log_message(level, message):
    """
    Appends a log message with level and timestamp, both printing to terminal
    and feeding the web UI console.
    """
    ts = datetime.now().strftime("%H:%M:%S")
    full_msg = f"[{ts}] [{level}] {message}"
    print(full_msg)
    
    with state_lock:
        state.app_logs.append(full_msg)
        if len(state.app_logs) > 300:
            state.app_logs.pop(0)

# ==========================================
# 2. SCRIP MASTER LOOKUP ENGINE
# ==========================================
def download_scrip_master(filename="OpenAPIScripMaster.json"):
    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    log_message("INFO", "Downloading latest Scrip Master from Angel One...")
    try:
        import urllib.request
        urllib.request.urlretrieve(url, filename)
        log_message("SUCCESS", "Scrip Master downloaded successfully!")
        return True
    except Exception as e:
        log_message("ERROR", f"Failed to download Scrip Master: {e}")
        return False

class ScripMasterLookup:
    def __init__(self, filename="OpenAPIScripMaster.json"):
        self.filename = filename
        self.index = {}
        self.sorted_nfo_expiries = []
        self.sorted_mcx_expiries = []
        self.nfo_symbols = []
        self.mcx_symbols = []
        self.load_and_index()
        
    def load_and_index(self):
        download_needed = False
        if not os.path.exists(self.filename):
            download_needed = True
        else:
            file_time = os.path.getmtime(self.filename)
            # 24 hours = 86400 seconds
            if time.time() - file_time > 86400:
                download_needed = True
                
        if download_needed:
            download_scrip_master(self.filename)
            
        log_message("INFO", f"Loading Scrip Master file '{self.filename}'...")
        t0 = time.time()
        if not os.path.exists(self.filename):
            log_message("CRITICAL", f"Scrip Master file '{self.filename}' not found! Please place it in the same directory.")
            # We don't crash, but wait for it.
            return
            
        try:
            with open(self.filename, 'r') as f:
                data = json.load(f)
            log_message("SUCCESS", f"Loaded {len(data)} scrips in {time.time()-t0:.2f} seconds. Building index...")
            
            t1 = time.time()
            nfo_expiries_set = set()
            mcx_expiries_set = set()
            nfo_symbols_set = set()
            mcx_symbols_set = set()
            
            self.token_index = {}
            self.symbol_index = {}
            
            for x in data:
                exch_seg = x.get('exch_seg')
                name = x.get('name', '').upper()
                symbol = x.get('symbol', '').upper()
                expiry = x.get('expiry', '').upper()
                token = x.get('token')
                
                lotsize_val = 1
                try:
                    lotsize_val = int(x.get('lotsize', 1) or 1)
                except Exception:
                    pass
                    
                contract_info = {
                    'token': token,
                    'symbol': symbol,
                    'name': name,
                    'lotsize': lotsize_val,
                    'exch_seg': exch_seg,
                    'expiry': expiry,
                }
                
                if exch_seg in ('NFO', 'MCX'):
                    if symbol.endswith(('CE', 'PE')):
                        try:
                            strike = int(round(float(x['strike']) / 100.0))
                        except Exception:
                            continue
                        opt_type = symbol[-2:]
                        contract_info['strike'] = strike
                        contract_info['opt_type'] = opt_type
                        
                        self.index[(name, expiry, strike, opt_type)] = {
                            'token': token,
                            'symbol': symbol,
                            'lotsize': lotsize_val,
                            'exch_seg': exch_seg
                        }
                        if expiry:
                            if exch_seg == 'NFO':
                                nfo_symbols_set.add(name)
                                if name in ('NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY'):
                                    nfo_expiries_set.add(expiry)
                            elif exch_seg == 'MCX':
                                mcx_symbols_set.add(name)
                                if name in ('GOLD', 'SILVER', 'CRUDEOIL', 'NATURALGAS', 'GOLDM', 'SILVERM', 'CRUDEOILM', 'NATGASMINI'):
                                    mcx_expiries_set.add(expiry)
                    elif symbol.endswith('FUT'):
                        contract_info['opt_type'] = 'FUT'
                        self.index[(name, expiry, 'FUT')] = {
                            'token': token,
                            'symbol': symbol,
                            'lotsize': lotsize_val,
                            'exch_seg': exch_seg
                        }
                elif exch_seg == 'NSE':
                    if symbol == f"{name}-EQ" or symbol == name:
                        contract_info['opt_type'] = 'STOCK'
                        self.index[(name, 'STOCK')] = {
                            'token': token,
                            'symbol': symbol,
                            'lotsize': lotsize_val,
                            'exch_seg': exch_seg
                        }
                        
                if token:
                    if exch_seg:
                        self.token_index[(str(exch_seg).upper(), str(token))] = contract_info
                    self.token_index[str(token)] = contract_info
                if symbol:
                    self.symbol_index[str(symbol).upper()] = contract_info
                        
            # Sort NFO expiries chronologically
            parsed_nfo_dates = []
            for exp in nfo_expiries_set:
                try:
                    dt = datetime.strptime(exp, "%d%b%Y").date()
                    if dt >= date.today():
                        parsed_nfo_dates.append((dt, exp))
                except Exception:
                    continue
            parsed_nfo_dates.sort(key=lambda item: item[0])
            
            # Format sorted NFO expiries for the dropdowns (e.g. 25-Jun-2026)
            self.sorted_nfo_expiries = []
            for item in parsed_nfo_dates[:35]:
                dt, exp = item
                self.sorted_nfo_expiries.append(dt.strftime("%d-%b-%Y"))
    
            # Sort MCX expiries chronologically
            parsed_mcx_dates = []
            for exp in mcx_expiries_set:
                try:
                    dt = datetime.strptime(exp, "%d%b%Y").date()
                    if dt >= date.today():
                        parsed_mcx_dates.append((dt, exp))
                except Exception:
                    continue
            parsed_mcx_dates.sort(key=lambda item: item[0])
            
            # Format sorted MCX expiries for the dropdowns (e.g. 16-Jun-2026)
            self.sorted_mcx_expiries = []
            for item in parsed_mcx_dates[:35]:
                dt, exp = item
                self.sorted_mcx_expiries.append(dt.strftime("%d-%b-%Y"))
                
            self.nfo_symbols = sorted(list(nfo_symbols_set))
            self.mcx_symbols = sorted(list(mcx_symbols_set))
    
            log_message("SUCCESS", f"Indexed {len(self.index)} contracts in {time.time()-t1:.2f} seconds.")
            log_message("INFO", f"Loaded {len(self.sorted_nfo_expiries)} NFO expiries and {len(self.sorted_mcx_expiries)} MCX expiries dynamically.")
            log_message("INFO", f"Gathered {len(self.nfo_symbols)} NFO symbols and {len(self.mcx_symbols)} MCX symbols successfully.")
        except Exception as e:
            log_message("ERROR", f"Exception reading Scrip Master: {e}")

    def lookup(self, name, expiry, strike, opt_type):
        if not name or not expiry or strike is None or not opt_type:
            return None
        name = str(name).strip().upper()
        expiry = str(expiry).strip().upper()
        try:
            strike = int(round(float(strike)))
        except Exception:
            return None
        opt_type = str(opt_type).strip().upper()
        return self.index.get((name, expiry, strike, opt_type))

    def find_contract_by_position(self, pos, broker_name):
        # 1. Try by token + exchange (for Angel One)
        if broker_name == "ANGEL_ONE":
            token = pos.get("symboltoken")
            exchange = pos.get("exchange")
            if token and exchange:
                exch_upper = str(exchange).upper()
                # Check exact match
                key = (exch_upper, str(token))
                if key in self.token_index:
                    return self.token_index[key]
                # Check MCX/NCO mapping fallback
                if exch_upper in ("MCX", "NCO"):
                    for alt_exch in ("MCX", "NCO"):
                        alt_key = (alt_exch, str(token))
                        if alt_key in self.token_index:
                            return self.token_index[alt_key]

        # 2. Try by tradingsymbol / symbol
        symbol = pos.get("tradingsymbol") or pos.get("symbol")
        if symbol:
            symbol_upper = str(symbol).upper()
            if ":" in symbol_upper:
                symbol_upper = symbol_upper.split(":")[1]
            if symbol_upper in self.symbol_index:
                return self.symbol_index[symbol_upper]
                
        # 3. Last fallback: search token index
        token = pos.get("symboltoken") or pos.get("instrument_token") or pos.get("token")
        exchange = pos.get("exchange") or pos.get("exch_seg")
        if token:
            if exchange:
                exch_upper = str(exchange).upper()
                key = (exch_upper, str(token))
                if key in self.token_index:
                    return self.token_index[key]
                if exch_upper in ("MCX", "NCO"):
                    for alt_exch in ("MCX", "NCO"):
                        alt_key = (alt_exch, str(token))
                        if alt_key in self.token_index:
                            return self.token_index[alt_key]
            # Global fallback
            if str(token) in self.token_index:
                return self.token_index[str(token)]
            
        return None

    def lookup_underlying(self, name, expiry=None):
        if not name:
            return None
        name = str(name).strip().upper()
        if expiry:
            norm_expiry = normalize_expiry(expiry)
            res = self.index.get((name, norm_expiry, 'FUT'))
            if res:
                return res
            # Fallback: Find nearest matching future expiry (within 45 days for NFO/MCX offset)
            try:
                opt_dt = datetime.strptime(norm_expiry, "%d%b%Y")
                best_fut = None
                best_diff = 99999
                for key, contract in self.index.items():
                    if len(key) == 3 and key[0] == name and key[2] == 'FUT':
                        fut_exp = key[1]
                        try:
                            fut_dt = datetime.strptime(fut_exp, "%d%b%Y")
                            diff = abs((fut_dt - opt_dt).days)
                            if diff < best_diff and diff <= 45:
                                best_diff = diff
                                best_fut = contract
                        except Exception:
                            continue
                if best_fut:
                    return best_fut
            except Exception:
                pass
        res = self.index.get((name, 'STOCK'))
        if res:
            return res
        return None

# ==========================================
# 3. SELF-HEALING LOGIN & AUTHENTICATION
# ==========================================
# ==========================================
# 3. SAAS CLIENT CONFIGURATION HELPERS
# ==========================================
def load_client_config():
    if os.path.exists(state.CLIENT_CONFIG_FILE):
        try:
            with open(state.CLIENT_CONFIG_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            log_message("ERROR", f"Failed to load client config: {e}")
    return None

def save_client_config(config_data):
    try:
        with open(state.CLIENT_CONFIG_FILE, "w") as f:
            json.dump(config_data, f, indent=4)
        return True
    except Exception as e:
        log_message("ERROR", f"Failed to save client config: {e}")
        return False

# NOTE: Yahoo Finance fetch removed by design. All rates must come from the connected broker API.

def safe_float(val):
    if val is None:
        return 0.0
    try:
        val_str = str(val).strip()
        if not val_str:
            return 0.0
        return float(val_str)
    except ValueError:
        return 0.0

# ==========================================
# 3b. UNIFIED MULTI-BROKER CLIENT INTERFACE
# ==========================================
class UnifiedBrokerClient:
    def __init__(self):
        self.broker = "NONE"
        self.connected = False
        self.profile = {"name": "Not Logged In", "client_code": "----", "broker": "NONE"}
        self.client_obj = None
        self.mode = "REAL"
        # Zerodha-specific kite object (separate from client_obj which holds raw session)
        self._kite_obj = None
        self.cached_funds = {"available": 0.0, "used": 0.0, "total": 0.0, "pnl": 0.0}
        self.last_funds_fetch_time = 0.0
        self.cached_positions = []
        self.last_positions_fetch_time = 0.0
        self.cached_orders = []
        self.last_orders_fetch_time = 0.0
        # Zerodha WebSocket and Instrument Map cache
        self._zerodha_ticks = {}
        self._zerodha_subscribed = set()
        self._zerodha_token_map = {}
        self._zerodha_ticker = None
        # Angel One WebSocket Ticker cache
        self._angel_ticks = {}
        self._angel_subscribed = set()
        self._angel_ticker = None
        # TrueData WebSocket Ticker cache
        self._truedata_ticks = {}
        self._truedata_subscribed = set()
        self._truedata_ticker = None
        # Fyers WebSocket Ticker cache
        self._fyers_ticks = {}
        self._fyers_subscribed = set()
        self._fyers_ticker = None
        self.api_lock = threading.Lock()

    def connect(self, config):
        if not config:
            self.connected = False
            return False

        self.broker = config.get("selected_broker", "ANGEL_ONE").upper().strip()
        creds = config.get("credentials", {})

        if self.broker == "ANGEL_ONE":
            return self._connect_angel_one(config, creds)
        elif self.broker == "ZERODHA":
            return self._connect_zerodha(config, creds)
        elif self.broker == "FYERS":
            return self._connect_fyers(config, creds)
        elif self.broker == "GROWW":
            return self._connect_groww(config, creds)
        return False

    def disconnect(self):
        self.connected = False
        self.broker = "NONE"
        self.profile = {"name": "Not Logged In", "client_code": "----", "broker": "NONE"}
        self.client_obj = None
        self._kite_obj = None
        
        # Stop Zerodha WebSocket Ticker if running
        if hasattr(self, "_zerodha_ticker") and self._zerodha_ticker is not None:
            try:
                self._zerodha_ticker.close()
            except Exception:
                pass
            self._zerodha_ticker = None

        # Stop Angel One WebSocket Ticker if running
        if hasattr(self, "_angel_ticker") and self._angel_ticker is not None:
            try:
                self._angel_ticker.close_connection()
            except Exception:
                pass
            self._angel_ticker = None
            
        # Stop TrueData WebSocket Ticker if running
        if hasattr(self, "_truedata_ticker") and self._truedata_ticker is not None:
            try:
                self._truedata_ticker.disconnect()
            except Exception:
                pass
            self._truedata_ticker = None
            
        # Clean caches
        if hasattr(self, "_zerodha_ticks"):
            self._zerodha_ticks.clear()
        if hasattr(self, "_zerodha_subscribed"):
            self._zerodha_subscribed.clear()
        if hasattr(self, "_zerodha_token_map"):
            self._zerodha_token_map.clear()
        if hasattr(self, "_angel_ticks"):
            self._angel_ticks.clear()
        if hasattr(self, "_angel_subscribed"):
            self._angel_subscribed.clear()
            
        if hasattr(self, "_truedata_ticks"):
            self._truedata_ticks.clear()
        if hasattr(self, "_truedata_subscribed"):
            self._truedata_subscribed.clear()
        if hasattr(self, "_fyers_ticks"):
            self._fyers_ticks.clear()
        if hasattr(self, "_fyers_subscribed"):
            self._fyers_subscribed.clear()
        log_message("WARNING", "Unified Broker Client disconnected.")

    def start_truedata_ticker(self, config=None, creds=None):
        """Initializes TrueData WebSocket Live Ticker Feed."""
        if hasattr(self, "_truedata_ticker") and self._truedata_ticker is not None:
            return

        try:
            from truedata_ws.websocket.TD import TD
        except ImportError:
            log_message("WARNING", "truedata_ws module not available. TrueData will not be used.")
            return

        td_user = "Trial123"
        td_pass = "mohd123"
        td_port = 8086

        try:
            log_message("INFO", "Initializing TrueData WebSocket Live Ticker Feed...")
            td_obj = TD(td_user, td_pass, live_port=td_port)

            @td_obj.trade_callback
            def on_trade(msg):
                try:
                    symbol = msg.symbol
                    if symbol:
                        ltp = float(msg.ltp or 0.0)
                        bid = float(msg.best_bid_price or ltp)
                        ask = float(msg.best_ask_price or ltp)
                        
                        existing = self._truedata_ticks.get(symbol, {})
                        self._truedata_ticks[symbol] = {
                            "ltp": ltp if ltp > 0 else existing.get("ltp", 0.0),
                            "bid": bid if bid > 0 else existing.get("bid", ltp),
                            "ask": ask if ask > 0 else existing.get("ask", ltp),
                            "timestamp": time.time()
                        }
                except Exception:
                    pass

            @td_obj.bidask_callback
            def on_bidask(msg):
                try:
                    symbol = msg.symbol
                    if symbol:
                        bid = float(msg.bid[0][0]) if (msg.bid and len(msg.bid) > 0 and msg.bid[0]) else 0.0
                        ask = float(msg.ask[0][0]) if (msg.ask and len(msg.ask) > 0 and msg.ask[0]) else 0.0
                        
                        existing = self._truedata_ticks.get(symbol, {})
                        curr_ltp = existing.get("ltp", 0.0)
                        if curr_ltp == 0.0 and bid > 0:
                            curr_ltp = round((bid + ask) / 2, 2) if ask > 0 else bid

                        self._truedata_ticks[symbol] = {
                            "ltp": curr_ltp,
                            "bid": bid if bid > 0 else existing.get("bid", 0.0),
                            "ask": ask if ask > 0 else existing.get("ask", 0.0),
                            "timestamp": time.time()
                        }
                except Exception:
                    pass

            self._truedata_ticker = td_obj
            log_message("SUCCESS", "TrueData Live WebSocket initialized (Trades + Bid/Ask feeds active).")
        except Exception as e:
            log_message("WARNING", f"Could not start TrueData WebSocket Ticker: {e}")

    def start_angel_ticker(self, config=None, creds=None):
        """Initializes Angel One High-Speed SmartWebSocketV2 Live Ticker Feed."""
        if hasattr(self, "_angel_ticker") and self._angel_ticker is not None:
            return

        try:
            from SmartApi.smartWebSocketV2 import SmartWebSocketV2
        except ImportError:
            log_message("WARNING", "SmartWebSocketV2 module not available. Angel One will fall back to REST API.")
            return

        tokens = {}
        if os.path.exists(state.TOKENS_FILE):
            try:
                with open(state.TOKENS_FILE, "r") as f:
                    tokens = json.load(f)
            except Exception:
                pass

        auth_token = tokens.get("jwtToken")
        raw_jwt = auth_token.replace("Bearer ", "") if auth_token else ""

        if not creds and isinstance(config, dict):
            creds = config.get("credentials", {})
        if not creds:
            creds = {}

        api_key = creds.get("api_key", "").strip()
        client_code = creds.get("client_code", "").strip() or self.profile.get("client_code", "").strip()
        feed_token = tokens.get("feedToken", "").strip() or (getattr(self.client_obj, 'feed_token', '') if self.client_obj else "")

        if not raw_jwt or not api_key or not client_code or not feed_token:
            log_message("WARNING", f"Angel One WebSocket missing params (jwt={bool(raw_jwt)}, key={bool(api_key)}, code={bool(client_code)}, feed={bool(feed_token)})")
            return

        try:
            log_message("INFO", "Initializing Angel One SmartWebSocketV2 Live Ticker Feed...")
            sws = SmartWebSocketV2(raw_jwt, api_key, client_code, feed_token)

            def on_data(wsapp, data):
                if not isinstance(data, dict):
                    return
                token = str(data.get("token", "")).strip()
                if not token:
                    return

                raw_ltp = data.get("last_traded_price", 0)
                ltp = float(raw_ltp) / 100.0 if raw_ltp > 0 else 0.0

                buy_list = []
                for item in data.get("best_5_buy_data", []):
                    p = float(item.get("price", 0)) / 100.0
                    q = int(item.get("quantity", 0))
                    if p > 0:
                        buy_list.append({"price": p, "quantity": q})

                sell_list = []
                for item in data.get("best_5_sell_data", []):
                    p = float(item.get("price", 0)) / 100.0
                    q = int(item.get("quantity", 0))
                    if p > 0:
                        sell_list.append({"price": p, "quantity": q})

                bid = buy_list[0]["price"] if buy_list else ltp
                ask = sell_list[0]["price"] if sell_list else ltp

                self._angel_ticks[token] = {
                    "ltp": ltp,
                    "bid": bid or ltp,
                    "ask": ask or ltp,
                    "buy_depth": buy_list,
                    "sell_depth": sell_list,
                    "timestamp": time.time()
                }

            def on_open(wsapp):
                log_message("SUCCESS", "Angel One High-Speed Live WebSocket streaming connected successfully!")
                if self._angel_subscribed:
                    tokens_by_exch = {}
                    for (exch_type, tok) in list(self._angel_subscribed):
                        if exch_type not in tokens_by_exch:
                            tokens_by_exch[exch_type] = []
                        tokens_by_exch[exch_type].append(tok)
                    token_list = [{"exchangeType": exch, "tokens": toks} for exch, toks in tokens_by_exch.items()]
                    try:
                        sws.subscribe("tradehub_resub", 3, token_list)
                        log_message("INFO", f"Angel One WebSocket resubscribed {len(self._angel_subscribed)} tokens.")
                    except Exception as e:
                        log_message("WARNING", f"Angel One WebSocket resubscribe error: {e}")

            def on_error(wsapp, error):
                log_message("WARNING", f"Angel One Live WebSocket error: {error}")

            def on_close(wsapp):
                log_message("WARNING", "Angel One Live WebSocket connection closed.")

            sws.on_data = on_data
            sws.on_open = on_open
            sws.on_error = on_error
            sws.on_close = on_close

            t = threading.Thread(target=sws.connect, daemon=True)
            t.start()
            self._angel_ticker = sws
        except Exception as e:
            log_message("WARNING", f"Could not start Angel One WebSocket Ticker: {e}")

    def _connect_angel_one(self, config, creds):
        api_key = creds.get("api_key", "").strip()
        username = creds.get("client_code", "").strip()
        pwd = creds.get("password", "").strip()
        totp_seed = creds.get("totp_seed", "").strip()
        entered_otp = creds.pop("entered_otp", "").strip()
        
        if not api_key or not username or not pwd:
            log_message("ERROR", "Angel One credentials missing required fields.")
            self.profile["error_details"] = "API Key, Client Code, and MPIN are required."
            return False

        # Use a larger timeout (default 25 seconds) to avoid connect/read timeouts under slow network or broker API congestion
        api_timeout = 25
        if isinstance(config, dict):
            api_timeout = config.get("api_timeout") or creds.get("api_timeout") or 25
        obj = SmartConnect(api_key=api_key, timeout=api_timeout)
        
        # Check tokens.json for active cached session (only if no new OTP entered)
        if not entered_otp and os.path.exists(state.TOKENS_FILE):
            try:
                with open(state.TOKENS_FILE, "r") as f:
                    tokens = json.load(f)
                
                raw_jwt = tokens["jwtToken"].replace("Bearer ", "")
                obj.setAccessToken(raw_jwt)
                obj.setRefreshToken(tokens["refreshToken"])
                if tokens.get("feedToken"):
                    obj.setFeedToken(tokens["feedToken"])
                
                profile = obj.getProfile(raw_jwt)
                if profile and profile.get("status") is True and profile.get("data") and profile['data'].get('clientcode'):
                    log_message("SUCCESS", f"Resumed Angel One session for {profile['data']['name']} ({profile['data']['clientcode']})")
                    self.profile = {
                        "name": profile['data']['name'],
                        "client_code": profile['data']['clientcode'],
                        "broker": "Angel One"
                    }
                    self.connected = True
                    self.mode = "REAL"
                    self.client_obj = obj
                    # self.start_angel_ticker(config, creds)  # Disabled to enforce pure TrueData WebSocket
                    self.start_truedata_ticker(config, creds)
                    return True
                else:
                    log_message("WARNING", "Cached Angel One tokens expired or invalid. Clearing tokens.json...")
                    try:
                        if os.path.exists(state.TOKENS_FILE):
                            os.remove(state.TOKENS_FILE)
                    except Exception:
                        pass
            except Exception as e:
                log_message("WARNING", f"Error reading tokens.json: {e}")
                try:
                    if os.path.exists(state.TOKENS_FILE):
                        os.remove(state.TOKENS_FILE)
                except Exception:
                    pass
                
        try:
            log_message("INFO", "Logging in to Angel One using OTP code...")
            
            # If the user entered a dynamic 6-digit OTP code in Step 4, use it directly!
            if entered_otp and len(entered_otp) == 6 and entered_otp.isdigit():
                totp_now = entered_otp
                log_message("INFO", f"Using entered dynamic OTP/2FA code: {totp_now}")
            elif totp_seed:
                # Generate from saved TOTP seed if present (for background auto-reconnects)
                try:
                    totp_now = pyotp.TOTP(totp_seed.replace(" ", "")).now()
                except Exception as totp_err:
                    log_message("ERROR", f"Invalid TOTP Seed provided for Angel One: {totp_err}")
                    self.profile["error_details"] = f"Invalid TOTP Key/Seed: {str(totp_err)}"
                    return False
            else:
                log_message("ERROR", "No active OTP/2FA PIN or TOTP seed provided.")
                self.profile["error_details"] = "Please enter a valid 6-digit OTP/2FA code from your Authenticator app."
                return False
                
            data = obj.generateSession(username, pwd, totp_now)
            
            if data and data.get("status"):
                log_message("SUCCESS", "Angel One Connection Successful!")
                tokens = {
                    "jwtToken": data['data']['jwtToken'],
                    "refreshToken": data['data']['refreshToken'],
                    "feedToken": data['data'].get('feedToken') or obj.getfeedToken()
                }
                with open(state.TOKENS_FILE, "w") as f:
                    json.dump(tokens, f, indent=4)
                    
                raw_jwt = tokens["jwtToken"].replace("Bearer ", "")
                obj.setAccessToken(raw_jwt)
                obj.setRefreshToken(tokens["refreshToken"])
                
                profile = obj.getProfile(raw_jwt)
                name = profile['data']['name'] if (profile and profile.get("status") is True and profile.get("data")) else config.get("client_name", "Demo User")
                self.profile = {
                    "name": name,
                    "client_code": username,
                    "broker": "Angel One"
                }
                self.connected = True
                self.mode = "REAL"
                self.client_obj = obj
                # self.start_angel_ticker(config, creds)  # Disabled to enforce pure TrueData WebSocket
                self.start_truedata_ticker(config, creds)
                return True
            else:
                err_msg = data.get("message", "Invalid API Key, MPIN or OTP/2FA PIN.")
                log_message("ERROR", f"Angel One API Handshake failed: {data}")
                self.profile["error_details"] = err_msg
                self.connected = False
                return False
        except Exception as e:
            err_msg = str(e)
            log_message("ERROR", f"Exception during Angel One Authentication: {err_msg}")
            self.profile["error_details"] = err_msg
            self.connected = False
            return False

    def _connect_zerodha(self, config, creds):
        """Real Zerodha Kite authentication using requests-based browser simulation."""
        if not ZERODHA_AVAILABLE:
            self.profile["error_details"] = "kiteconnect library is not installed. Run: pip install kiteconnect"
            return False

        api_key = creds.get("api_key", "").strip()
        user_id = creds.get("client_code", "").strip().upper()
        password = creds.get("password", "").strip()
        entered_otp = creds.pop("entered_otp", "").strip()   # TOTP from Step 4
        # Also accept a pre-stored access_token for session resumption
        stored_access_token = creds.get("zerodha_access_token", "").strip()

        if not api_key or not user_id or not password:
            log_message("ERROR", "Zerodha connection failed: API Key, User ID, and Password are required.")
            self.profile["error_details"] = "Kite API Key, User ID, and Password are all required."
            return False

        # Close existing WebSocket ticker if running
        if hasattr(self, "_zerodha_ticker") and self._zerodha_ticker is not None:
            try:
                self._zerodha_ticker.close()
            except Exception:
                pass
            self._zerodha_ticker = None

        def apply_enctoken_patch(kite_instance, token):
            orig_request = kite_instance.reqsession.request
            def custom_request(method, url, *args, **kwargs):
                if url.startswith("https://api.kite.trade/"):
                    url = url.replace("https://api.kite.trade/", "https://kite.zerodha.com/oms/")
                headers = kwargs.get("headers", {}) or {}
                headers["Authorization"] = f"enctoken {token}"
                kwargs["headers"] = headers
                return orig_request(method, url, *args, **kwargs)
            kite_instance.reqsession.request = custom_request

        def download_zerodha_instruments():
            self._zerodha_token_map = {}
            log_message("INFO", "Downloading Zerodha instruments map (NFO, MCX, NSE)...")
            try:
                for segment in ["NFO", "MCX", "NSE"]:
                    r = requests.get(f"https://api.kite.trade/instruments/{segment}", timeout=15)
                    if r.status_code == 200:
                        f = io.StringIO(r.text)
                        reader = csv.reader(f)
                        next(reader)  # Skip header
                        for row in reader:
                            if len(row) > 11:
                                token = int(row[0])
                                symbol = row[2]
                                exchange = row[11]
                                self._zerodha_token_map[f"{exchange}:{symbol}"] = token
                log_message("SUCCESS", f"Zerodha instruments map loaded successfully: {len(self._zerodha_token_map)} records.")
                return True
            except Exception as e:
                log_message("ERROR", f"Failed to download Zerodha instruments: {e}")
                return False

        def start_zerodha_ticker(token):
            self._zerodha_ticks = {}
            self._zerodha_subscribed = set()
            encoded_enctoken = urllib.parse.quote(token)
            
            ticker = KiteTicker(api_key="kitefront", access_token=f"{encoded_enctoken}&user_id={user_id}", root="wss://ws.kite.trade")
            
            orig_create_connection = ticker._create_connection
            def custom_create_connection(url, **kwargs):
                headers = kwargs.get("headers", {}) or {}
                headers["Origin"] = "https://kite.zerodha.com"
                if "X-Kite-Version" in headers:
                    del headers["X-Kite-Version"]
                kwargs["headers"] = headers
                return orig_create_connection(url, **kwargs)
            ticker._create_connection = custom_create_connection

            def on_ticks(ws, ticks):
                for tick in ticks:
                    tok = tick.get("instrument_token")
                    if tok:
                        self._zerodha_ticks[tok] = tick

            def on_connect(ws, response):
                log_message("SUCCESS", "Zerodha Live Ticker WebSocket connected successfully.")
                if self._zerodha_subscribed:
                    sub_list = list(self._zerodha_subscribed)
                    ws.subscribe(sub_list)
                    ws.set_mode(ws.MODE_FULL, sub_list)

            def on_close(ws, code, reason):
                log_message("WARNING", f"Zerodha Live Ticker closed: {code} - {reason}")

            def on_error(ws, code, reason):
                log_message("ERROR", f"Zerodha Live Ticker error: {code} - {reason}")

            ticker.on_ticks = on_ticks
            ticker.on_connect = on_connect
            ticker.on_close = on_close
            ticker.on_error = on_error
            
            ticker.connect(threaded=True)
            self._zerodha_ticker = ticker

        # Try restoring from stored access token first
        if stored_access_token and not entered_otp:
            try:
                kite = KiteConnect(api_key=api_key)
                apply_enctoken_patch(kite, stored_access_token)
                profile = kite.profile()
                if profile and profile.get("user_id"):
                    log_message("SUCCESS", f"Resumed Zerodha session for {profile['user_name']} ({profile['user_id']})")
                    self.profile = {
                        "name": profile["user_name"],
                        "client_code": profile["user_id"],
                        "broker": "Zerodha Kite"
                    }
                    self.connected = True
                    self.mode = "REAL"
                    self._kite_obj = kite
                    self.client_obj = kite
                    download_zerodha_instruments()
                    start_zerodha_ticker(stored_access_token)
                    return True
            except Exception as e:
                log_message("WARNING", f"Stored Zerodha token invalid, re-authenticating: {e}")

        if not entered_otp:
            self.profile["error_details"] = "Please enter your TOTP code from your authenticator app."
            return False

        try:
            log_message("INFO", f"Authenticating Zerodha user {user_id}...")
            sess = requests.Session()
            sess.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded"
            })

            # Step 1: Submit user_id + password
            r1 = sess.post(
                "https://kite.zerodha.com/api/login",
                data={"user_id": user_id, "password": password},
                timeout=10
            )
            d1 = r1.json()
            if d1.get("status") != "success":
                err = d1.get("message") or d1.get("error") or "Invalid User ID or Password."
                log_message("ERROR", f"Zerodha Step 1 failed: {err}")
                self.profile["error_details"] = f"Login failed: {err}"
                return False

            request_id = d1["data"]["request_id"]
            log_message("INFO", "Zerodha Step 1 OK. Submitting TOTP...")

            # Step 2: Submit TOTP
            r2 = sess.post(
                "https://kite.zerodha.com/api/twofa",
                data={
                    "user_id": user_id,
                    "request_id": request_id,
                    "twofa_value": entered_otp,
                    "twofa_type": "totp",
                    "skip_totp": "false"
                },
                timeout=10
            )
            d2 = r2.json()
            if d2.get("status") != "success":
                err = d2.get("message") or d2.get("error") or "Invalid TOTP code."
                log_message("ERROR", f"Zerodha TOTP verification failed: {err}")
                self.profile["error_details"] = f"TOTP verification failed: {err}"
                return False

            log_message("INFO", "Zerodha TOTP OK. Requesting KiteConnect session token...")

            # Step 3: Get the kite_enctoken from session cookie
            enctoken = None
            for cookie in sess.cookies:
                if cookie.name == "enctoken":
                    enctoken = cookie.value
                    break

            if not enctoken:
                enctoken = d2.get("data", {}).get("enctoken") or d2.get("data", {}).get("access_token")

            if not enctoken:
                self.profile["error_details"] = "Authentication succeeded but session token was not returned. Please check your Kite API subscription."
                log_message("ERROR", "Zerodha auth OK but enctoken missing from cookies.")
                return False

            kite = KiteConnect(api_key=api_key)
            apply_enctoken_patch(kite, enctoken)

            # Verify by fetching profile
            profile = kite.profile()
            if not profile or not profile.get("user_id"):
                raise Exception("Profile fetch failed after login verification.")
            user_name = profile.get("user_name", user_id)

            log_message("SUCCESS", f"Zerodha Real Authentication Successful for {user_name} ({user_id})!")
            self.profile = {
                "name": user_name,
                "client_code": user_id,
                "broker": "Zerodha Kite"
            }
            self.connected = True
            self.mode = "REAL"
            self._kite_obj = kite
            self.client_obj = kite
            download_zerodha_instruments()
            start_zerodha_ticker(enctoken)
            # Save enctoken back to creds for session resumption
            creds["zerodha_access_token"] = enctoken
            return True

        except requests.exceptions.ConnectionError:
            err = "Could not reach Zerodha servers. Check internet connection."
            log_message("ERROR", err)
            self.profile["error_details"] = err
            return False
        except Exception as e:
            err = str(e)
            log_message("ERROR", f"Zerodha Authentication Exception: {err}")
            self.profile["error_details"] = err
            return False

    def _fyers_post_request(self, path, json_payload, headers=None, timeout=10):
        """Perform a Fyers POST request with sequential subdomain fallback.
        Retries across api-t1.fyers.in, api.fyers.in, and api-t2.fyers.in.
        """
        if path.startswith("/vagator/"):
            subdomains = ["api-t2.fyers.in", "api-t1.fyers.in", "api.fyers.in"]
        else:
            subdomains = ["api-t1.fyers.in", "api.fyers.in", "api-t2.fyers.in"]

        last_resp = None
        for sub in subdomains:
            url = f"https://{sub}{path}"
            try:
                log_message("INFO", f"Trying Fyers POST request to {url}...")
                r = requests.post(url, json=json_payload, headers=headers, timeout=timeout)
                
                # Treat 404, or 500/502/503 with "Invalid Request" as unsupported route on this subdomain
                is_invalid_route = (r.status_code == 404) or (
                    r.status_code in (500, 502, 503) and "invalid request" in r.text.lower()
                )
                if is_invalid_route:
                    log_message("WARNING", f"Fyers endpoint {url} is not supported on this server. Trying next subdomain...")
                    last_resp = r
                    continue
                return r
            except Exception as e:
                log_message("WARNING", f"Fyers request to {url} failed: {e}")
        if last_resp is not None:
            return last_resp
        raise requests.exceptions.ConnectionError("All Fyers subdomains failed to respond.")


    def _connect_fyers(self, config, creds):
        """Real Fyers API v3 programmatic authentication."""
        if not FYERS_AVAILABLE:
            self.profile["error_details"] = "fyers-apiv3 library is not installed. Run: pip install fyers-apiv3"
            return False

        app_id = creds.get("api_key", "").strip()         # Fyers App ID e.g. "XY12345-100"
        app_secret = creds.get("app_secret", "").strip()  # Fyers App Secret (API Secret Key)
        client_id = creds.get("client_code", "").strip()  # Fyers Client ID e.g. "XY12345"
        mpin = creds.get("password", "").strip()           # Fyers MPIN (4-6 digit PIN)
        entered_otp = creds.pop("entered_otp", "").strip() # OTP sent to phone (Step 4)
        fyers_req_key = creds.get("fyers_request_key", "").strip() # Stored from Step 3 initiation
        # Optional: stored access_token for session resumption
        stored_token = creds.get("fyers_access_token", "").strip()

        if not app_id or not client_id or not mpin:
            self.profile["error_details"] = "Fyers App ID, Client ID, and MPIN are required."
            return False

        # Try resuming stored access token
        if stored_token and not entered_otp:
            try:
                raw_token = stored_token.split(":")[-1] if ":" in stored_token else stored_token
                fyers = FyersModel.FyersModel(client_id=app_id, is_async=False, token=raw_token, log_path="")
                resp = fyers.get_profile()
                if resp and resp.get("code") == 200:
                    pdata = resp.get("data", {})
                    log_message("SUCCESS", f"Resumed Fyers session for {pdata.get('name', client_id)}")
                    self.profile = {
                        "name": pdata.get("name", client_id),
                        "client_code": client_id,
                        "broker": "Fyers API v3"
                    }
                    self.connected = True
                    self.mode = "REAL"
                    self.client_obj = fyers
                    return True
            except Exception as e:
                log_message("WARNING", f"Stored Fyers token invalid, re-authenticating: {e}")

        totp_seed = creds.get("totp_seed", "").strip()
        if not entered_otp and totp_seed:
            try:
                # Request server date header from Fyers to eliminate clock drift
                r_time = requests.get("https://api-t1.fyers.in/vagator/v2/send_login_otp", timeout=3)
                server_time_str = r_time.headers.get("Date")
                if server_time_str:
                    import email.utils
                    server_time = email.utils.parsedate_to_datetime(server_time_str).timestamp()
                    entered_otp = pyotp.TOTP(totp_seed).at(server_time)
                    log_message("INFO", f"Generated Fyers TOTP using Fyers server time ({server_time_str}): {entered_otp}")
                else:
                    entered_otp = pyotp.TOTP(totp_seed).now()
                    log_message("INFO", f"Generated Fyers TOTP using local clock: {entered_otp}")
            except Exception as e:
                log_message("WARNING", f"Failed to sync clock with Fyers server: {e}")
                try:
                    entered_otp = pyotp.TOTP(totp_seed).now()
                    log_message("INFO", f"Generated Fyers TOTP using fallback local clock: {entered_otp}")
                except Exception as ex:
                    log_message("WARNING", f"Failed to generate fallback Fyers TOTP: {ex}")

        if not entered_otp:
            self.profile["error_details"] = "Please enter the OTP sent to your registered mobile number."
            return False

        if not fyers_req_key:
            self.profile["error_details"] = "Authentication session expired. Please go back and start from Step 3 again."
            return False

        try:
            import hashlib
            log_message("INFO", f"Fyers Step 2: Verifying OTP for {client_id}...")

            # Step 2: Verify the phone OTP (or TOTP if totp_enabled)
            r_verify = self._fyers_post_request(
                "/vagator/v2/verify_otp",
                json_payload={"request_key": fyers_req_key, "otp": entered_otp},
                timeout=10
            )
            try:
                d_verify = r_verify.json()
            except json.JSONDecodeError:
                log_message("ERROR", "Fyers verify_otp returned non-JSON. Likely rate-limited/blocked.")
                self.profile["error_details"] = "Fyers server rejected the request. You are temporarily blocked due to too many failed OTP attempts. Please wait 5 minutes and try again."
                return False

            log_message("INFO", f"Fyers verify_otp response: s={d_verify.get('s')} code={d_verify.get('code')} has_key={bool(d_verify.get('request_key'))}")
            # Fyers uses 's': 'ok' as success marker, code varies (200, 1043, etc)
            if d_verify.get("s") != "ok" or not d_verify.get("request_key"):
                err = d_verify.get("message") or d_verify.get("error") or "Invalid OTP / TOTP code."
                if "invalid totp" in err.lower() or "totp" in err.lower():
                    err += ". Tip: Ensure your mobile phone and system clock are perfectly synchronized to the correct local time."
                log_message("ERROR", f"Fyers OTP verification failed: {d_verify}")
                self.profile["error_details"] = f"OTP/TOTP verification failed: {err}"
                return False

            req_key_2 = d_verify["request_key"]
            log_message("INFO", "Fyers OTP OK. Verifying MPIN...")

            # Step 3: Verify MPIN (PIN)
            r_pin = self._fyers_post_request(
                "/vagator/v2/verify_pin",
                json_payload={
                    "request_key": req_key_2,
                    "identity_type": "pin",
                    "identifier": mpin,
                    "recaptcha_token": ""
                },
                timeout=10
            )
            try:
                d_pin = r_pin.json()
            except json.JSONDecodeError:
                log_message("ERROR", "Fyers verify_pin returned non-JSON. Likely rate-limited/blocked.")
                self.profile["error_details"] = "Fyers server is rate-limiting pin verification. Please wait 5 minutes."
                return False

            log_message("INFO", f"Fyers verify_pin response: s={d_pin.get('s')} code={d_pin.get('code')}")
            if d_pin.get("s") != "ok":
                err = d_pin.get("message") or d_pin.get("error") or "Invalid MPIN / PIN."
                log_message("ERROR", f"Fyers MPIN verification failed: {d_pin}")
                self.profile["error_details"] = f"MPIN verification failed: {err}"
                return False

            fyers_access_token = d_pin.get("data", {}).get("access_token", "")
            log_message("INFO", f"Fyers MPIN OK. Access Token present: {bool(fyers_access_token)}")
            if not fyers_access_token:
                log_message("ERROR", f"Fyers verify_pin succeeded but data.access_token is empty! Full response: {d_pin}")
                self.profile["error_details"] = f"Fyers PIN verified but no access token returned. Response: {d_pin}"
                return False

            log_message("INFO", "Fyers MPIN OK. Requesting auth code...")

            # Step 4: Get auth code using the access_token from verify_pin
            redirect_uri = creds.get("redirect_uri", "").strip() or "https://trade.fyers.in/api-login/redirect-uri/index.html"
            core_app_id = app_id.split("-")[0] if "-" in app_id else app_id
            log_message("INFO", f"Fyers Token Exchange: fyers_id={client_id}, core_app_id={core_app_id}")
            r_token = self._fyers_post_request(
                "/api/v3/token",
                json_payload={
                    "fyers_id": client_id,
                    "app_id": core_app_id,
                    "redirect_uri": redirect_uri,
                    "appType": "100",
                    "code_challenge": "",
                    "state": "None",
                    "nonce": "",
                    "response_type": "code",
                    "create_cookie": True,
                    "access_token": fyers_access_token
                },
                headers={"Authorization": fyers_access_token},
                timeout=10
            )
            try:
                d_auth = r_token.json()
            except json.JSONDecodeError:
                log_message("ERROR", f"Fyers api/v3/token returned non-JSON! Status={r_token.status_code}, Response={r_token.text[:1000]}")
                self.profile["error_details"] = f"Fyers session token exchange failed (Status {r_token.status_code}). Please check your API App ID & App Secret or try again."
                return False

            auth_url = d_auth.get("Url", "")
            log_message("INFO", f"Fyers token response: s={d_auth.get('s')} has_url={bool(auth_url)}")
            if not auth_url or "auth_code=" not in auth_url:
                err = d_auth.get("message") or f"Could not obtain auth code. Full response: {d_auth}"
                log_message("ERROR", f"Fyers auth code failed: {d_auth}")
                self.profile["error_details"] = f"Could not get auth code: {err}"
                return False

            auth_code = auth_url.split("auth_code=")[1].split("&")[0]
            log_message("INFO", "Fyers auth code received. Generating access token...")

            # Step 5: Exchange auth code for access token using SDK
            if not app_secret:
                self.profile["error_details"] = "Fyers App Secret Key is required to exchange auth code for access token."
                return False

            session = FyersModel.SessionModel(
                client_id=app_id,
                secret_key=app_secret,
                redirect_uri=redirect_uri,
                response_type="code",
                grant_type="authorization_code"
            )
            session.set_token(auth_code)
            token_resp = session.generate_token()
            final_token = token_resp.get("access_token", "")
            if not final_token:
                err = token_resp.get("message") or "Failed to generate Fyers access token."
                log_message("ERROR", f"Fyers token generation failed: {token_resp}")
                self.profile["error_details"] = err
                return False

            # Create Fyers SDK instance with clean final token
            fyers = FyersModel.FyersModel(client_id=app_id, is_async=False, token=final_token, log_path="")

            # Verify by fetching profile
            try:
                pdata = fyers.get_profile().get("data", {})
                user_name = pdata.get("name", client_id)
            except Exception:
                user_name = client_id

            log_message("SUCCESS", f"Fyers Real Authentication Successful for {user_name} ({client_id})!")
            self.profile = {
                "name": user_name,
                "client_code": client_id,
                "broker": "Fyers API v3"
            }
            self.connected = True
            self.mode = "REAL"
            self.client_obj = fyers
            # Save token for session resumption
            creds["fyers_access_token"] = final_token
            return True

        except requests.exceptions.ConnectionError:
            err = "Could not reach Fyers servers. Check internet connection."
            log_message("ERROR", err)
            self.profile["error_details"] = err
            return False
        except Exception as e:
            err = str(e)
            log_message("ERROR", f"Fyers Authentication Exception: {err}")
            self.profile["error_details"] = err
            return False

    def _connect_groww(self, config, creds):
        """Groww broker - requires their trading API credentials."""
        api_key = creds.get("api_key", "").strip()
        client_code = creds.get("client_code", "").strip()
        pwd = creds.get("password", "").strip()
        entered_otp = creds.pop("entered_otp", "").strip()

        if not api_key or not client_code or not pwd:
            log_message("ERROR", "Groww connection failed: Missing required fields.")
            self.profile["error_details"] = "Groww API Key, Client Code, and Password are required."
            return False

        try:
            log_message("INFO", f"Authenticating Groww user {client_code}...")
            r = requests.post(
                "https://groww.in/v1/api/user/login",
                json={"email": client_code, "password": pwd, "otp": entered_otp},
                headers={"X-App-Token": api_key, "Content-Type": "application/json"},
                timeout=10
            )
            data = r.json()
            if r.status_code != 200 or not data.get("accessToken"):
                err = data.get("message") or data.get("error") or "Invalid credentials. Please verify Groww API Key, email, and password."
                log_message("ERROR", f"Groww authentication failed: {err}")
                self.profile["error_details"] = err
                return False

            access_token = data["accessToken"]
            user_name = data.get("name", client_code)

            log_message("SUCCESS", f"Groww Authentication Successful for {user_name} ({client_code})!")
            self.profile = {
                "name": user_name,
                "client_code": client_code,
                "broker": "Groww"
            }
            self.connected = True
            self.mode = "REAL"
            self.client_obj = {"access_token": access_token, "api_key": api_key}
            return True
        except requests.exceptions.ConnectionError:
            err = "Could not reach Groww servers. Check internet connection."
            log_message("ERROR", err)
            self.profile["error_details"] = err
            return False
        except Exception as e:
            err = str(e)
            log_message("ERROR", f"Groww Authentication Exception: {err}")
            self.profile["error_details"] = err
            return False

    def get_market_data(self, exchange_tokens):
        """
        Fetches LIVE market data ONLY from the connected broker's real API.
        exchange_tokens: dict in format {exch: [token1, token2]}
        Returns: {token: {"ltp": ltp, "bid": bid, "ask": ask}}
        """
        if not self.connected:
            return {}

        if self.broker == "ANGEL_ONE" and isinstance(self.client_obj, SmartConnect):
            EXCH_TYPE_MAP = {
                "NSE": 1, "NSE_CM": 1,
                "NFO": 2, "NSE_FO": 2,
                "BSE": 3, "BSE_CM": 3,
                "BFO": 4, "BSE_FO": 4,
                "MCX": 5, "MCX_FO": 5
            }

            new_tokens_by_exch = {}
            all_requested_tokens = []

            for exch_str, tok_list in exchange_tokens.items():
                exch_type = EXCH_TYPE_MAP.get(str(exch_str).upper(), 2)
                for tok in tok_list:
                    tok_str = str(tok).strip()
                    all_requested_tokens.append(tok_str)
                    if (exch_type, tok_str) not in self._angel_subscribed:
                        if exch_type not in new_tokens_by_exch:
                            new_tokens_by_exch[exch_type] = []
                        new_tokens_by_exch[exch_type].append(tok_str)
                        self._angel_subscribed.add((exch_type, tok_str))

            if new_tokens_by_exch and hasattr(self, "_angel_ticker") and self._angel_ticker is not None:
                token_list = [{"exchangeType": exch, "tokens": toks} for exch, toks in new_tokens_by_exch.items()]
                try:
                    self._angel_ticker.subscribe("tradehub_sub", 3, token_list)
                    log_message("INFO", f"Subscribed Angel One Live WebSocket to tokens: {new_tokens_by_exch}")
                except Exception as e:
                    log_message("WARNING", f"Angel One WebSocket subscription failed: {e}")

            td_mapping = {}
            if getattr(self, "_truedata_ticker", None) is not None:
                td_mapping = self._get_truedata_symbols(exchange_tokens)
                td_syms_to_sub = []
                for tok_str, sym in td_mapping.items():
                    if sym and sym not in self._truedata_subscribed:
                        td_syms_to_sub.append(sym)
                        self._truedata_subscribed.add(sym)
                
                if td_syms_to_sub:
                    try:
                        self._truedata_ticker.start_live_data(td_syms_to_sub)
                        log_message("INFO", f"Subscribed TrueData WebSocket to tokens: {td_syms_to_sub}")
                    except Exception as e:
                        log_message("WARNING", f"TrueData WebSocket subscription failed: {e}")

            result = {}
            missing_tokens = []

            for tok in all_requested_tokens:
                tick = None
                if getattr(self, "_truedata_ticker", None) is not None:
                    sym = td_mapping.get(tok)
                    if sym:
                        tick = self._truedata_ticks.get(sym)
                        
                # Angel One fallback strictly disabled as requested
                # if not tick:
                #     tick = self._angel_ticks.get(tok)
                    
                if tick:
                    result[tok] = tick
                else:
                    missing_tokens.append(tok)

            if missing_tokens:
                missing_exchange_tokens = {}
                for exch_str, tok_list in exchange_tokens.items():
                    sub_missing = [str(t) for t in tok_list if str(t) in missing_tokens]
                    if sub_missing:
                        missing_exchange_tokens[exch_str] = sub_missing
                if missing_exchange_tokens:
                    try:
                        res = self.client_obj.getMarketData(mode="FULL", exchangeTokens=missing_exchange_tokens)
                        rest_data = extract_market_data(res)
                        result.update(rest_data)
                        for tok_k, v in rest_data.items():
                            self._angel_ticks[str(tok_k)] = v
                    except Exception as e:
                        log_message("WARNING", f"Angel One REST API fallback error: {e}")
            return result

        elif self.broker == "ZERODHA" and self._kite_obj is not None:
            zerodha_instruments = self._get_zerodha_instruments(exchange_tokens)
            if not zerodha_instruments:
                raise Exception("Could not map strategy legs to Zerodha instruments. Please re-add your legs.")
            
            result = {}
            tokens_to_subscribe = []
            
            # Map symbol -> token
            for orig_token, kite_sym in zerodha_instruments.items():
                z_token = self._zerodha_token_map.get(kite_sym)
                if z_token:
                    if z_token not in self._zerodha_subscribed:
                        tokens_to_subscribe.append(z_token)
                        self._zerodha_subscribed.add(z_token)

            # Subscribe to any new tokens in live WebSocket
            if tokens_to_subscribe and hasattr(self, "_zerodha_ticker") and self._zerodha_ticker is not None and self._zerodha_ticker.is_connected():
                try:
                    self._zerodha_ticker.subscribe(tokens_to_subscribe)
                    self._zerodha_ticker.set_mode(self._zerodha_ticker.MODE_FULL, tokens_to_subscribe)
                    log_message("INFO", f"Subscribed Zerodha Ticker to: {tokens_to_subscribe}")
                except Exception as e:
                    log_message("WARNING", f"Failed to subscribe Zerodha Ticker tokens {tokens_to_subscribe}: {e}")

            # Fetch ticks from local cache
            for orig_token, kite_sym in zerodha_instruments.items():
                z_token = self._zerodha_token_map.get(kite_sym)
                if not z_token:
                    continue

                # Wait up to 1.5s for the first tick to arrive
                start_w = time.time()
                while z_token not in self._zerodha_ticks and (time.time() - start_w < 1.5):
                    time.sleep(0.05)

                q = self._zerodha_ticks.get(z_token)
                if q:
                    ltp = float(q.get("last_price", 0.0))
                    depth = q.get("depth", {})
                    buy_list = depth.get("buy", [])
                    sell_list = depth.get("sell", [])
                    bid = float(buy_list[0].get("price", ltp)) if buy_list else ltp
                    ask = float(sell_list[0].get("price", ltp)) if sell_list else ltp
                    result[orig_token] = {
                        "ltp": ltp,
                        "bid": bid or ltp,
                        "ask": ask or ltp,
                        "buy_depth": buy_list,
                        "sell_depth": sell_list
                    }
            return result

        elif self.broker == "FYERS" and self.client_obj is not None:
            fyers_symbols = self._get_fyers_symbols(exchange_tokens)
            if not fyers_symbols:
                raise Exception("Could not map strategy legs to Fyers symbols. Please re-add your legs.")
            syms_list = list(fyers_symbols.values())
            resp = self.client_obj.quotes(data={"symbols": ",".join(syms_list)})
            result = {}
            if resp and resp.get("code") == 200:
                for q in resp.get("d", []):
                    sym = q.get("n", "")
                    v = q.get("v", {})
                    ltp = float(v.get("lp", 0.0))
                    bid = float(v.get("bid", ltp))
                    ask = float(v.get("ask", ltp))
                    buy_list = [{"price": bid, "quantity": 999999}] if bid else []
                    sell_list = [{"price": ask, "quantity": 999999}] if ask else []
                    # Map back to original token
                    for tok, fsym in fyers_symbols.items():
                        if fsym == sym:
                            result[tok] = {
                                "ltp": ltp,
                                "bid": bid or ltp,
                                "ask": ask or ltp,
                                "close": float(v.get("prev_close_price", 0.0)),
                                "buy_depth": buy_list,
                                "sell_depth": sell_list
                            }
            return result

        else:
            raise Exception(f"No live market data available for broker '{self.broker}'. Please ensure you are connected.")

    def _get_truedata_symbols(self, exchange_tokens):
        """Convert Angel One tokens to TrueData symbols."""
        mapping = {}
        if not lookup_engine:
            return mapping
            
        td_index_names = {
            "NIFTY": "NIFTY 50",
            "BANKNIFTY": "NIFTY BANK",
            "FINNIFTY": "NIFTY FIN SERVICE",
            "MIDCPNIFTY": "NIFTY MID SELECT",
            "SENSEX": "SENSEX"
        }
        
        spot_tokens = {
            "99926000": "NIFTY 50",
            "99926037": "NIFTY BANK",
            "99926074": "NIFTY FIN SERVICE",
            "99926002": "NIFTY MID SELECT",
            "99919000": "SENSEX"
        }
            
        for exch, tokens in exchange_tokens.items():
            for token in tokens:
                token_str = str(token).strip()
                if token_str in spot_tokens:
                    mapping[token_str] = spot_tokens[token_str]
                    continue
                    
                if not lookup_engine:
                    continue
                    
                found = False
                for key, contract in lookup_engine.index.items():
                    if contract["token"] == token_str:
                        found = True
                        if len(key) == 4:
                            (name, expiry_norm, strike, opt_type) = key
                        elif len(key) == 3:
                            (name, expiry_norm, opt_type) = key
                            strike = 0
                        elif len(key) == 2:
                            (name, opt_type) = key
                            expiry_norm = ""
                            strike = 0
                        else:
                            continue

                        sym = contract.get("symbol", "")
                        
                        try:
                            if opt_type == 'STOCK':
                                sym = name
                                if sym in td_index_names:
                                    sym = td_index_names[sym]
                            elif opt_type == 'FUT':
                                dt = datetime.strptime(expiry_norm, "%d%b%Y")
                                sym = f"{name}{dt.strftime('%y%b').upper()}FUT"
                            else:
                                dt = datetime.strptime(expiry_norm, "%d%b%Y")
                                sym = f"{name}{dt.strftime('%y%m%d')}{int(strike)}{opt_type}"
                        except Exception as e:
                            pass
                            
                        mapping[token_str] = sym
                        break
        return mapping

    def _get_fyers_symbols(self, exchange_tokens):
        """Convert Angel One exchange_tokens dict to Fyers symbol strings."""
        mapping = {}  # token -> fyers_symbol
        if not lookup_engine:
            return mapping
        for exch, tokens in exchange_tokens.items():
            for token in tokens:
                for key, contract in lookup_engine.index.items():
                    if contract["token"] == token:
                        if len(key) == 4:
                            (name, expiry_norm, strike, opt_type) = key
                        elif len(key) == 3:
                            (name, expiry_norm, opt_type) = key
                            strike = 0
                        elif len(key) == 2:
                            (name, opt_type) = key
                            expiry_norm = ""
                            strike = 0
                        else:
                            continue
                            
                        try:
                            fyers_exchange = "MCX" if exch == "MCX" else "NSE"
                            if opt_type == 'STOCK':
                                sym = f"NSE:{name}-EQ"
                            elif opt_type == 'FUT':
                                dt = datetime.strptime(expiry_norm, "%d%b%Y")
                                yy = dt.strftime("%y")
                                mmm = dt.strftime("%b").upper()
                                sym = f"{fyers_exchange}:{name}{yy}{mmm}FUT"
                            else:
                                dt = datetime.strptime(expiry_norm, "%d%b%Y")
                                yy = dt.strftime("%y")  # '26'
                                
                                # Determine if it's a monthly contract
                                is_monthly = True
                                if fyers_exchange == "NSE" and name in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"):
                                    # Find all expiries in the same month/year for this symbol
                                    month_expiries = []
                                    current_month = dt.month
                                    current_year = dt.year
                                    for k in lookup_engine.index.keys():
                                        k_name, k_expiry, k_strike, k_opt = k if len(k) == 4 else (None, None, None, None)
                                        if k_name == name:
                                            try:
                                                k_dt = datetime.strptime(k_expiry, "%d%b%Y")
                                                if k_dt.month == current_month and k_dt.year == current_year:
                                                    month_expiries.append(k_dt)
                                            except Exception:
                                                continue
                                    if month_expiries:
                                        is_monthly = (dt == max(month_expiries))
                                
                                if is_monthly:
                                    mmm = dt.strftime("%b").upper()  # 'JUN'
                                    sym = f"{fyers_exchange}:{name}{yy}{mmm}{int(strike)}{opt_type}"
                                else:
                                    # Weekly contract format
                                    m_code = str(dt.month)
                                    if dt.month == 10: m_code = "O"
                                    elif dt.month == 11: m_code = "N"
                                    elif dt.month == 12: m_code = "D"
                                    dd = dt.strftime("%d")  # '18'
                                    sym = f"{fyers_exchange}:{name}{yy}{m_code}{dd}{int(strike)}{opt_type}"
                        except Exception as e:
                            log_message("WARNING", f"Error parsing Fyers symbol: {e}")
                            sym = f"NSE:{name}{expiry_norm[:7]}{strike}{opt_type}"
                            
                        mapping[token] = sym
                        break
        return mapping

    def _get_zerodha_instruments(self, exchange_tokens):
        """Convert Angel One tokens to Zerodha tradingsymbol strings."""
        mapping = {}  # token -> zerodha_symbol
        if not lookup_engine:
            return mapping
        for exch, tokens in exchange_tokens.items():
            for token in tokens:
                for key, contract in lookup_engine.index.items():
                    if contract["token"] == token:
                        if len(key) == 4:
                            (name, expiry_norm, strike, opt_type) = key
                        elif len(key) == 3:
                            (name, expiry_norm, opt_type) = key
                            strike = 0
                        elif len(key) == 2:
                            (name, opt_type) = key
                            expiry_norm = ""
                            strike = 0
                        else:
                            continue
                            
                        try:
                            zerodha_exchange = "MCX" if exch == "MCX" else "NFO"
                            if opt_type == 'STOCK':
                                sym = f"NSE:{name}"
                            elif opt_type == 'FUT':
                                dt = datetime.strptime(expiry_norm, "%d%b%Y")
                                yy = dt.strftime("%y")
                                mmm = dt.strftime("%b").upper()
                                sym = f"{zerodha_exchange}:{name}{yy}{mmm}FUT"
                            else:
                                dt = datetime.strptime(expiry_norm, "%d%b%Y")
                                yy = dt.strftime("%y")  # '26'
                                
                                if exch == "MCX":
                                    mmm = dt.strftime("%b").upper()  # 'JUN'
                                    sym = f"{zerodha_exchange}:{name}{yy}{mmm}{int(strike)}{opt_type}"
                                else:
                                    # Determine if it's a monthly contract
                                    is_monthly = True
                                    if name in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"):
                                        # Find all expiries in the same month/year for this symbol
                                        month_expiries = []
                                        current_month = dt.month
                                        current_year = dt.year
                                        for k in lookup_engine.index.keys():
                                            k_name, k_expiry, k_strike, k_opt = k if len(k) == 4 else (None, None, None, None)
                                            if k_name == name:
                                                try:
                                                    k_dt = datetime.strptime(k_expiry, "%d%b%Y")
                                                    if k_dt.month == current_month and k_dt.year == current_year:
                                                        month_expiries.append(k_dt)
                                                except Exception:
                                                    continue
                                        if month_expiries:
                                            is_monthly = (dt == max(month_expiries))
                                    
                                    if is_monthly:
                                        mmm = dt.strftime("%b").upper()  # 'JUN'
                                        sym = f"{zerodha_exchange}:{name}{yy}{mmm}{int(strike)}{opt_type}"
                                    else:
                                        # Weekly contract format
                                        m = dt.month
                                        if m == 10: m_code = "O"
                                        elif m == 11: m_code = "N"
                                        elif m == 12: m_code = "D"
                                        else: m_code = str(m)
                                        dd = dt.strftime("%d")  # '18'
                                        sym = f"{zerodha_exchange}:{name}{yy}{m_code}{dd}{int(strike)}{opt_type}"
                        except Exception as e:
                            log_message("WARNING", f"Error parsing Zerodha symbol: {e}")
                            sym = f"NFO:{name}{expiry_norm[:7]}{strike}{opt_type}"
                            
                        mapping[token] = sym
                        break
        return mapping

    def get_funds(self):
        if not self.connected:
            return {"available": 0.0, "used": 0.0, "total": 0.0, "pnl": 0.0}
        
        now = time.time()
        if now - self.last_funds_fetch_time < 20.0:
            return self.cached_funds

        with self.api_lock:
            # Double check cache
            if time.time() - self.last_funds_fetch_time < 20.0:
                return self.cached_funds
                
            now = time.time()
            try:
                funds = {"available": 0.0, "used": 0.0, "total": 0.0, "pnl": 0.0}
                if self.broker == "ANGEL_ONE" and isinstance(self.client_obj, SmartConnect):
                    res = self.client_obj.rmsLimit()
                    if res and res.get("status") is True:
                        data = res.get("data", {})
                        available = safe_float(data.get("net") or data.get("availablecash") or data.get("availablelimitmargin"))
                        used = safe_float(data.get("utiliseddebits") or data.get("utilised") or data.get("utilized"))
                        
                        # Calculate live positions P&L for Angel One
                        pnl = 0.0
                        try:
                            pos_res = self.client_obj.position()
                            if pos_res and pos_res.get("status") is True:
                                for p in (pos_res.get("data") or []):
                                    pnl += safe_float(p.get("pnl") or p.get("netpnl") or p.get("realised") or p.get("unrealised"))
                        except Exception as ex:
                            log_message("WARNING", f"Failed to fetch Angel One positions for P&L: {ex}")
                            pnl = safe_float(data.get("m2mrealized")) + safe_float(data.get("m2munrealized"))
                            if pnl == 0.0:
                                pnl = safe_float(data.get("m2m"))
                            
                        funds = {
                            "available": available,
                            "used": used,
                            "total": available + used,
                            "pnl": pnl
                        }
                elif self.broker == "ZERODHA" and self._kite_obj is not None:
                    res = self._kite_obj.margins()
                    
                    equity_data = res.get("equity") or {}
                    eq_avail = safe_float(equity_data.get("available", {}).get("live_balance"))
                    eq_used = safe_float(equity_data.get("utilised", {}).get("debits"))
                    eq_utilised = equity_data.get("utilised") or {}
                    eq_m2m = safe_float(eq_utilised.get("m2m_realised")) + safe_float(eq_utilised.get("m2m_unrealised"))
                    
                    comm_data = res.get("commodity") or {}
                    comm_avail = safe_float(comm_data.get("available", {}).get("live_balance"))
                    comm_used = safe_float(comm_data.get("utilised", {}).get("debits"))
                    comm_utilised = comm_data.get("utilised") or {}
                    comm_m2m = safe_float(comm_utilised.get("m2m_realised")) + safe_float(comm_utilised.get("m2m_unrealised"))
                    
                    # Sum positions P&L for Zerodha Kite (Only sum 'net' positions to avoid double-counting today's trades)
                    pnl = 0.0
                    try:
                        pos_res = self._kite_obj.positions()
                        if pos_res and "net" in pos_res:
                            for p in pos_res.get("net", []):
                                pnl += safe_float(p.get("m2m") or p.get("pnl") or p.get("unrealised") or p.get("realised"))
                    except Exception as ex:
                        log_message("WARNING", f"Failed to fetch Zerodha positions for P&L: {ex}")
                        pnl = eq_m2m + comm_m2m
                        
                    funds = {
                        "available": eq_avail + comm_avail,
                        "used": eq_used + comm_used,
                        "total": eq_avail + comm_avail + eq_used + comm_used,
                        "pnl": pnl
                    }
                elif self.broker == "FYERS" and self.client_obj is not None:
                    res = self.client_obj.funds()
                    pnl = 0.0
                    try:
                        res_pos = self.client_obj.positions()
                        if res_pos and res_pos.get("code") == 200:
                            overall = res_pos.get("overall") or {}
                            pnl = safe_float(overall.get("pl_total"))
                    except Exception as ex:
                        log_message("WARNING", f"Failed to fetch Fyers positions P&L: {ex}")

                    if res and res.get("code") == 200:
                        fund_limits = res.get("fund_limit", [])
                        total = 0.0
                        used = 0.0
                        available = 0.0
                        for item in fund_limits:
                            title = item.get("title", "").lower()
                            eq_amt = safe_float(item.get("equityAmount"))
                            comm_amt = safe_float(item.get("commodityAmount"))
                            amt = eq_amt + comm_amt
                            if "total" in title:
                                total = amt
                            elif "utili" in title:
                                used = amt
                            elif "clear" in title or "avail" in title or "net" in title:
                                available = amt
                        if total == 0.0 and available > 0.0:
                            total = available + used
                        if available == 0.0 and total > 0.0:
                            available = total - used
                        funds = {
                            "available": available,
                            "used": used,
                            "total": total,
                            "pnl": pnl
                        }
                elif self.broker == "GROWW":
                    funds = {
                        "available": 100000.0,
                        "used": 0.0,
                        "total": 100000.0,
                        "pnl": 0.0
                    }
                self.cached_funds = funds
                self.last_funds_fetch_time = now
                return funds
            except Exception as e:
                log_message("WARNING", f"Failed to fetch funds/P&L for {self.broker}: {e}")
                self.last_funds_fetch_time = now - 10.0  # Wait 10s before retry (cooldown)
                return self.cached_funds

    def place_order(self, orderparams):
        if not self.connected:
            raise Exception("Broker disconnected.")

        if self.broker == "ANGEL_ONE" and isinstance(self.client_obj, SmartConnect):
            return self.client_obj.placeOrder(orderparams)

        elif self.broker == "ZERODHA" and self._kite_obj is not None:
            # Map Angel One token to Zerodha tradingsymbol dynamically
            token = orderparams["symboltoken"]
            exch = orderparams.get("exchange", "NFO")
            mapped = self._get_zerodha_instruments({exch: [token]})
            z_sym = mapped.get(token)
            if not z_sym:
                raise Exception(f"Could not map Angel One token {token} to Zerodha symbol.")
            if ":" in z_sym:
                z_sym = z_sym.split(":")[1]

            # Map order params to Kite format
            kite_params = {
                "tradingsymbol": z_sym,
                "exchange": exch,
                "transaction_type": orderparams["transactiontype"],
                "quantity": int(orderparams["quantity"]),
                "order_type": orderparams.get("ordertype", "MARKET"),
                "product": "CNC" if exch == "NSE" else "NRML",
            }
            order_id = self._kite_obj.place_order(variety="regular", **kite_params)
            log_message("SUCCESS", f"[ZERODHA] Order placed for {z_sym}. ID: {order_id}")
            return str(order_id)

        elif self.broker == "FYERS" and self.client_obj is not None:
            # Map Angel One token to Fyers symbol dynamically
            token = orderparams["symboltoken"]
            exch = orderparams.get("exchange", "NFO")
            mapped = self._get_fyers_symbols({exch: [token]})
            f_sym = mapped.get(token)
            if not f_sym:
                raise Exception(f"Could not map Angel One token {token} to Fyers symbol.")

            fyers_params = {
                "symbol": f_sym,
                "qty": int(orderparams["quantity"]),
                "type": 2,  # Market order
                "side": 1 if orderparams["transactiontype"] == "BUY" else -1,
                "productType": "CNC" if exch == "NSE" else "MARGIN",
                "limitPrice": 0,
                "stopPrice": 0,
                "validity": "DAY",
                "disclosedQty": 0,
                "offlineOrder": False,
                "orderTag": "TRADEHUB"
            }
            resp = self.client_obj.place_order(data=fyers_params)
            if resp and resp.get("code") == 200:
                order_id = resp.get("id", str(resp))
                log_message("SUCCESS", f"[FYERS] Order placed for {f_sym}. ID: {order_id}")
                return str(order_id)
            else:
                raise Exception(f"Fyers order failed: {resp}")

        else:
            raise Exception(f"Order placement not supported for broker '{self.broker}'. Please reconnect.")

    def get_order_book(self):
        if not self.connected:
            return []
        if self.broker == "ANGEL_ONE" and isinstance(self.client_obj, SmartConnect):
            try:
                res = self.client_obj.orderBook()
                if res and res.get("status") is True:
                    return res.get("data", [])
            except Exception:
                pass
        elif self.broker == "ZERODHA" and self._kite_obj is not None:
            try:
                return self._kite_obj.orders() or []
            except Exception:
                pass
        elif self.broker == "FYERS" and self.client_obj is not None:
            try:
                resp = self.client_obj.orderbook()
                if resp and resp.get("code") == 200:
                    return resp.get("orderBook", [])
            except Exception:
                pass
        return []

    def get_positions(self):
        if not self.connected:
            return []
        if self.broker == "ANGEL_ONE" and isinstance(self.client_obj, SmartConnect):
            try:
                res = self.client_obj.position()
                if res and res.get("status") is True:
                    return res.get("data") or []
            except Exception as e:
                log_message("WARNING", f"Failed to fetch Angel One positions: {e}")
        elif self.broker == "ZERODHA" and self._kite_obj is not None:
            try:
                res = self._kite_obj.positions()
                if res:
                    return res.get("net") or []
            except Exception as e:
                log_message("WARNING", f"Failed to fetch Zerodha positions: {e}")
        elif self.broker == "FYERS" and self.client_obj is not None:
            try:
                res = self.client_obj.positions()
                if res and res.get("code") == 200:
                    return res.get("netPositions") or []
            except Exception as e:
                log_message("WARNING", f"Failed to fetch Fyers positions: {e}")
        return []


    def get_orders(self):
        return []

    def _get_angel_one_margin(self, legs, strategy_lot):
        if not self.connected or not isinstance(self.client_obj, SmartConnect):
            return None
        positions = []
        for leg in legs:
            if not leg.get("token"):
                continue
            exch = leg.get("exch_seg", "NFO")
            lotsize = leg.get("lotsize", 1)
            qty = int(lotsize * leg["lot"] * strategy_lot)
            price = float(leg.get("ltp") or 0.0)
            action = leg.get("action", "BUY").upper()
            producttype = "DELIVERY" if exch == "NSE" else "CARRYFORWARD"
            positions.append({
                "exchange": exch,
                "qty": qty,
                "price": price,
                "productType": producttype,
                "producttype": producttype,
                "token": leg["token"],
                "tradeType": action,
                "tradetype": action,
                "ordertype": "MARKET",
                "orderType": "MARKET"
            })
        if not positions:
            return 0.0
        try:
            res = self.client_obj.getMarginApi({"positions": positions})
            if res and res.get("status") is True:
                data = res.get("data", {})
                total_margin = data.get("totalMargin") or data.get("totalRequiredMargin") or data.get("marginRequired")
                if total_margin is not None:
                    return float(total_margin)
                for k in ["total", "totalMarginRequired", "netRequired"]:
                    if k in data:
                        return float(data[k])
                span = float(data.get("spanMargin") or data.get("span_margin") or 0.0)
                exp = float(data.get("exposureMargin") or data.get("exposure_margin") or 0.0)
                if span > 0 or exp > 0:
                    return span + exp
        except Exception as e:
            log_message("WARNING", f"Angel One margin API failed: {e}")
        return None

    def _get_zerodha_margin(self, legs, strategy_lot):
        if not self.connected or self._kite_obj is None:
            return None
        basket_params = []
        for leg in legs:
            if not leg.get("token"):
                continue
            token = leg["token"]
            exch = leg.get("exch_seg", "NFO")
            mapped = self._get_zerodha_instruments({exch: [token]})
            z_sym = mapped.get(token)
            if not z_sym:
                continue
            if ":" in z_sym:
                z_sym = z_sym.split(":")[1]
                
            lotsize = leg.get("lotsize", 1)
            qty = int(lotsize * leg["lot"] * strategy_lot)
            action = leg.get("action", "BUY").upper()
            product = "CNC" if exch == "NSE" else "NRML"
            
            basket_params.append({
                "exchange": exch,
                "tradingsymbol": z_sym,
                "transaction_type": action,
                "variety": "regular",
                "product": product,
                "order_type": "MARKET",
                "quantity": qty,
                "price": 0.0
            })
        if not basket_params:
            return 0.0
        try:
            res = self._kite_obj.basket_order_margins(basket_params, mode="compact")
            if res:
                initial = res.get("initial", {}).get("total") or res.get("initial", {}).get("margin")
                if initial is not None:
                    return float(initial)
                final = res.get("final", {}).get("total") or res.get("final", {}).get("margin")
                if final is not None:
                    return float(final)
                total = res.get("total")
                if total is not None:
                    return float(total)
        except Exception as e:
            log_message("WARNING", f"Zerodha basket_order_margins failed: {e}")
            
        try:
            res = self._kite_obj.order_margins(basket_params)
            if res:
                total = 0.0
                for item in res:
                    total += float(item.get("total") or item.get("margin") or 0.0)
                return total
        except Exception as e:
            log_message("WARNING", f"Zerodha order_margins failed: {e}")
        return None

    def _get_fyers_margin(self, legs, strategy_lot):
        if not self.connected or self.client_obj is None:
            return None
        fyers_positions = []
        for leg in legs:
            if not leg.get("token"):
                continue
            token = leg["token"]
            exch = leg.get("exch_seg", "NFO")
            mapped = self._get_fyers_symbols({exch: [token]})
            f_sym = mapped.get(token)
            if not f_sym:
                continue
                
            lotsize = leg.get("lotsize", 1)
            qty = int(lotsize * leg["lot"] * strategy_lot)
            action = leg.get("action", "BUY").upper()
            side = 1 if action == "BUY" else -1
            productType = "CNC" if exch == "NSE" else "MARGIN"
            
            fyers_positions.append({
                "symbol": f_sym,
                "qty": qty,
                "side": side,
                "type": 2,
                "productType": productType
            })
        if not fyers_positions:
            return 0.0
        try:
            app_id = getattr(self.client_obj, "client_id", None)
            access_token = getattr(self.client_obj, "token", None)
            if not app_id or not access_token:
                config = load_client_config()
                if config:
                    creds = config.get("credentials", {})
                    app_id = creds.get("api_key")
                    access_token = creds.get("fyers_access_token")
            if not app_id or not access_token:
                return None
                
            headers = {
                "Authorization": f"{app_id}:{access_token}",
                "Content-Type": "application/json"
            }
            r = self._fyers_post_request(
                "/api/v3/multiorder/margin",
                json_payload={"data": fyers_positions},
                headers=headers,
                timeout=10
            )
            if r.status_code == 200:
                d = r.json()
                if d.get("code") == 200 or d.get("s") == "ok":
                    data = d.get("data", {})
                    total_margin = data.get("totalMargin") or data.get("total_margin") or data.get("marginRequired")
                    if total_margin is not None:
                        return float(total_margin)
                    margins = data.get("margin", [])
                    if isinstance(margins, list):
                        total = 0.0
                        for m in margins:
                            total += float(m.get("totalMargin") or m.get("margin") or 0.0)
                        return total
        except Exception as e:
            log_message("WARNING", f"Fyers margin API failed: {e}")
        return None

    def _estimate_fallback_margin(self, legs, strategy_lot):
        total_margin = 0.0
        symbol_legs = {}
        for leg in legs:
            name = leg.get("symbol", "")
            if not name:
                continue
            if name not in symbol_legs:
                symbol_legs[name] = []
            symbol_legs[name].append(leg)
            
        for name, s_legs in symbol_legs.items():
            buy_legs = [l for l in s_legs if l.get("action", "BUY").upper() == "BUY"]
            sell_legs = [l for l in s_legs if l.get("action", "BUY").upper() == "SELL"]
            has_hedge = len(buy_legs) > 0 and len(sell_legs) > 0
            
            for l in buy_legs:
                opt_type = l.get("opt_type", "").upper()
                lotsize = l.get("lotsize", 1)
                qty = float(lotsize * l.get("lot", 1.0) * strategy_lot)
                ltp = float(l.get("ltp") or 0.0)
                
                if opt_type in ("CE", "PE"):
                    total_margin += qty * ltp
                else:
                    if opt_type == "STOCK":
                        total_margin += qty * ltp
                    else: # FUT
                        total_margin += qty * ltp * 0.12
                        
            for l in sell_legs:
                opt_type = l.get("opt_type", "").upper()
                lotsize = l.get("lotsize", 1)
                qty = float(lotsize * l.get("lot", 1.0) * strategy_lot)
                ltp = float(l.get("ltp") or 0.0)
                exch = l.get("exch_seg", "NFO").upper()
                
                if opt_type in ("CE", "PE"):
                    lots = l.get("lot", 1.0) * strategy_lot
                    if exch == "MCX":
                        base_margin = 150000.0 * lots
                    elif any(idx in name.upper() for idx in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]):
                        base_margin = 120000.0 * lots
                    else:
                        base_margin = 200000.0 * lots
                    
                    if has_hedge:
                        total_margin += base_margin * 0.35
                    else:
                        total_margin += base_margin
                else:
                    if opt_type == "STOCK":
                        total_margin += qty * ltp * 0.25
                    else: # FUT
                        lots = l.get("lot", 1.0) * strategy_lot
                        if exch == "MCX":
                            base_margin = 150000.0 * lots
                        elif any(idx in name.upper() for idx in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]):
                            base_margin = 120000.0 * lots
                        else:
                            base_margin = 200000.0 * lots
                            
                        if has_hedge:
                            total_margin += base_margin * 0.35
                        else:
                            total_margin += base_margin
        return round(total_margin, 2)

    def calculate_strategy_margin(self, legs, strategy_lot=1.0):
        if not legs:
            return 0.0
        if self.connected:
            try:
                if self.broker == "ANGEL_ONE" and isinstance(self.client_obj, SmartConnect):
                    margin = self._get_angel_one_margin(legs, strategy_lot)
                    if margin is not None:
                        return margin
                elif self.broker == "ZERODHA" and self._kite_obj is not None:
                    margin = self._get_zerodha_margin(legs, strategy_lot)
                    if margin is not None:
                        return margin
                elif self.broker == "FYERS" and self.client_obj is not None:
                    margin = self._get_fyers_margin(legs, strategy_lot)
                    if margin is not None:
                        return margin
            except Exception as e:
                log_message("WARNING", f"Failed to fetch live margin from broker {self.broker}: {e}")
        return self._estimate_fallback_margin(legs, strategy_lot)

    def get_historical_candles(self, exch_seg, token, symbol, interval, from_date, to_date):
        """
        Fetches historical candles for a specific token/contract from the connected broker.
        Returns a list of candles: [{"time": unix_timestamp, "open": o, "high": h, "low": l, "close": c, "volume": v}]
        """
        if not self.connected or self.client_obj is None:
            return []
            
        try:
            if self.broker == "ANGEL_ONE" and isinstance(self.client_obj, SmartConnect):
                # Self-throttling rate limiter to keep Angel One requests under 3 calls per second (0.4s gap)
                with self.api_lock:
                    now = time.time()
                    last_fetch = getattr(self, "last_historical_fetch_time", 0.0)
                    elapsed = now - last_fetch
                    if elapsed < 0.4:
                        time.sleep(0.4 - elapsed)
                    self.last_historical_fetch_time = time.time()

                # Map interval
                tf_map = {
                    "1m": "ONE_MINUTE",
                    "5m": "FIVE_MINUTE",
                    "15m": "FIFTEEN_MINUTE",
                    "30m": "THIRTY_MINUTE",
                    "1h": "ONE_HOUR",
                    "1d": "ONE_DAY"
                }
                interval_mapped = tf_map.get(interval, "ONE_MINUTE")
                params = {
                    "exchange": exch_seg,
                    "symboltoken": token,
                    "interval": interval_mapped,
                    "fromdate": from_date.strftime("%Y-%m-%d %H:%M"),
                    "todate": to_date.strftime("%Y-%m-%d %H:%M")
                }
                res = self.client_obj.getCandleData(params)
                if res and res.get("status") is True and res.get("data"):
                    candles = []
                    for row in res["data"]:
                        # Parse date string (e.g. "2026-07-08T09:15:00+05:30")
                        dt_str = row[0]
                        if "+" in dt_str:
                            dt_str = dt_str.split("+")[0]
                        dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S")
                        ts = int(dt.timestamp())
                        candles.append({
                            "time": ts,
                            "open": float(row[1]),
                            "high": float(row[2]),
                            "low": float(row[3]),
                            "close": float(row[4]),
                            "volume": int(row[5])
                        })
                    return candles
                    
            elif self.broker == "ZERODHA" and self._kite_obj is not None:
                tf_map = {
                    "1m": "minute",
                    "5m": "5minute",
                    "15m": "15minute",
                    "30m": "30minute",
                    "1h": "60minute",
                    "1d": "day"
                }
                interval_mapped = tf_map.get(interval, "minute")
                res = self.client_obj.historical_data(
                    instrument_token=int(token),
                    from_date=from_date,
                    to_date=to_date,
                    interval=interval_mapped
                )
                if res:
                    candles = []
                    for row in res:
                        candles.append({
                            "time": int(row["date"].timestamp()),
                            "open": float(row["open"]),
                            "high": float(row["high"]),
                            "low": float(row["low"]),
                            "close": float(row["close"]),
                            "volume": int(row["volume"] or 0)
                        })
                    return candles
                    
            elif self.broker == "FYERS":
                tf_map = {
                    "1m": "1",
                    "5m": "5",
                    "15m": "15",
                    "30m": "30",
                    "1h": "60",
                    "1d": "D"
                }
                interval_mapped = tf_map.get(interval, "1")
                # Fyers expects exchange prefix in symbol
                fyers_sym = f"{exch_seg}:{symbol}"
                data = {
                    "symbol": fyers_sym,
                    "resolution": interval_mapped,
                    "date_format": "1",
                    "range_from": from_date.strftime("%Y-%m-%d"),
                    "range_to": to_date.strftime("%Y-%m-%d"),
                    "cont_flag": "1"
                }
                res = self.client_obj.history(data=data)
                if res and res.get("s") == "ok" and res.get("candles"):
                    candles = []
                    for row in res["candles"]:
                        candles.append({
                            "time": int(row[0]),
                            "open": float(row[1]),
                            "high": float(row[2]),
                            "low": float(row[3]),
                            "close": float(row[4]),
                            "volume": int(row[5])
                        })
                    return candles
        except Exception as e:
            log_message("WARNING", f"Error fetching historical candles from broker {self.broker}: {e}")
            
        return []



# Global Broker Wrapper Instantiation
state.unified_broker = UnifiedBrokerClient()

# ==========================================
# 4. UTILITIES & EXPIRY NORMALIZER
# ==========================================
def normalize_expiry(expiry_val):
    if not expiry_val:
        return ""
    
    val = str(expiry_val).strip().upper()
    val = val.replace("-", "").replace("/", "").replace(" ", "")
    
    # Robust regular expression matching for dynamic formats
    match = re.match(r"^(\d{1,2})([A-Z]{3})(\d{1,4})$", val)
    if match:
        day_str = match.group(1)
        month_str = match.group(2)
        year_str = match.group(3)
        
        if len(day_str) == 1:
            day_str = "0" + day_str
            
        if len(year_str) == 1:
            year_str = "202" + year_str
        elif len(year_str) == 2:
            year_str = "20" + year_str
        elif len(year_str) == 3:
            if year_str.startswith("20"):
                year_str = "202" + year_str[2:]  # '206' -> '2026'
            else:
                year_str = "20" + year_str[1:]
                
        return f"{day_str}{month_str}{year_str}"
        
    return val

def extract_market_data(api_response):
    market_data = {}
    if not api_response:
        return market_data
        
    status = api_response.get("status")
    success = api_response.get("success")
    msg = api_response.get("message", "")
    err_code = api_response.get("errorCode") or api_response.get("errorcode", "")
    
    if (status is False or success is False or "Invalid Token" in msg or "AG8001" in err_code):
        if "Invalid Token" in msg or "AG8001" in err_code or "expired" in msg.lower():
            raise Exception(f"Angel One session expired or token is invalid: {msg} (Code: {err_code})")
            
    if not status and not success:
        return market_data
        
    data = api_response.get("data")
    if not data or "fetched" not in data:
        return market_data
        
    for item in data["fetched"]:
        token = item.get("symbolToken")
        ltp = float(item.get("ltp", 0.0))
        
        bid = 0.0
        ask = 0.0
        depth = item.get("depth", {})
        
        buy_list = depth.get("buy", [])
        if buy_list and len(buy_list) > 0:
            bid = float(buy_list[0].get("price", 0.0))
            
        sell_list = depth.get("sell", [])
        if sell_list and len(sell_list) > 0:
            ask = float(sell_list[0].get("price", 0.0))
            
        if bid == 0.0:
            bid = ltp
        if ask == 0.0:
            ask = ltp
            
        market_data[token] = {
            "ltp": ltp,
            "bid": bid,
            "ask": ask,
            "close": float(item.get("close", 0.0)),
            "buy_depth": buy_list,
            "sell_depth": sell_list
        }
    return market_data

# ==========================================
# 5. ORDER PLACEMENT ENGINE (TRADE AUTOMATION)
# ==========================================
def execute_strategy_trade(obj, strategy, action_type):
    """
    Executes standard market orders for all legs in the strategy.
    action_type: "BUY" or "SELL"
    """
    legs = strategy["legs"]
    symbol = strategy["symbol"]
    
    if not legs:
        log_message("ERROR", f"Strategy {symbol} has no legs configured.")
        strategy["status"] = "Error: No legs configured."
        return False
        
    order_ids = []
    error_msg = None
    
    log_message("INFO", f"[TRADE] Executing Strategy {action_type} for {symbol}...")
    strategy["status"] = f"[PROCESSING] Placing {action_type} orders..."
    
    for leg in legs:
        if action_type == "BUY":
            leg_action = leg["action"]
        else: # "SELL"
            leg_action = "SELL" if leg["action"] == "BUY" else "BUY"
            
        token = leg["token"]
        trading_symbol = leg["symbol"]
        
        # Calculate quantity based on standard lotsize * leg weight * strategy multiplier
        lotsize = leg.get("lotsize", 1)
        strategy_mult = strategy.get("strategy_lot", 1.0)
        quantity = int(lotsize * leg["lot"] * strategy_mult)
        
        exch = leg.get("exch_seg", "NFO")
        product_type = "DELIVERY" if exch == "NSE" else "CARRYFORWARD"
        orderparams = {
            "variety": "NORMAL",
            "tradingsymbol": trading_symbol,
            "symboltoken": token,
            "transactiontype": leg_action,
            "exchange": exch,
            "ordertype": "MARKET",
            "producttype": product_type,
            "duration": "DAY",
            "price": "0",
            "quantity": str(quantity)
        }
        
        try:
            log_message("INFO", f"[TRADE] Placing order: {leg_action} {quantity} {trading_symbol} ({token})")
            orderId = obj.place_order(orderparams)
            if orderId:
                order_ids.append(orderId)
                order_status = "SUBMITTED"
                try:
                    orders = obj.get_order_book()
                    for o in orders:
                        if o.get("orderid") == orderId:
                            order_status = o.get("status", "SUBMITTED").upper()
                            if order_status == "REJECTED" and o.get("text"):
                                order_status += f" ({o.get('text')})"
                            break
                except Exception as e:
                    log_message("WARNING", f"Could not fetch order status: {e}")
                
                leg["status"] = f"Order Placed: {orderId} | Status: {order_status}"
            else:
                error_msg = f"Order failed for leg strike {leg['strike']}"
                leg["status"] = "Order Failed: No ID returned"
        except Exception as e:
            error_msg = f"Exception: {str(e)}"
            leg["status"] = f"Order Exception: {str(e)}"
            log_message("ERROR", f"Leg Order Exception: {e}")
            
    # Reset triggers to prevent double executing orders
    strategy["trade_action"] = ""
    strategy["execute_trigger"] = ""
    
    if order_ids:
        success_log = f"[SUCCESS] {action_type} executed! IDs: {', '.join(order_ids)}"
        strategy["status"] = success_log
        log_message("SUCCESS", f"Strategy Trade Success: {success_log}")
        
        # Wait 1.5 seconds for orders to fill and query average fill prices
        time.sleep(1.5)
        executed_prices = {}
        try:
            orders = obj.get_order_book()
            for o in orders:
                oid = str(o.get("orderid") or o.get("order_id") or o.get("id") or "")
                if oid in order_ids:
                    price = o.get("averageprice") or o.get("average_price") or o.get("avgPrice") or o.get("price")
                    try:
                        executed_prices[oid] = float(price)
                    except (ValueError, TypeError):
                        executed_prices[oid] = 0.0
        except Exception as e:
            log_message("WARNING", f"Could not fetch execution prices: {e}")
            
        total_executed_diff = 0.0
        for leg, oid in zip(legs, order_ids):
            price = executed_prices.get(str(oid))
            if price is not None and price > 0:
                leg["entry_price"] = price
            else:
                price = leg.get("ltp", 0.0)
                leg["entry_price"] = price
                
            sign = 1.0 if leg["action"] == "BUY" else -1.0
            total_executed_diff += sign * leg["lot"] * price
            
        # Update strategy position and average entry difference
        change_pos = strategy_mult if action_type == "BUY" else -strategy_mult
        current_pos = strategy.get("position", 0.0) or 0.0
        current_avg = strategy.get("avg_entry_diff", 0.0) or 0.0
        
        if current_pos == 0.0:
            new_pos = change_pos
            new_avg = total_executed_diff
        else:
            new_pos = current_pos + change_pos
            if new_pos == 0.0:
                new_avg = 0.0
            else:
                if (current_pos > 0 and change_pos > 0) or (current_pos < 0 and change_pos < 0):
                    new_avg = (current_pos * current_avg + change_pos * total_executed_diff) / new_pos
                else:
                    new_avg = current_avg  # Closing trades do not change average entry rate of remaining
                    
        strategy["position"] = round(new_pos, 2)
        strategy["avg_entry_diff"] = round(new_avg, 2)
        
        log_message("SUCCESS", f"Strategy {symbol} position updated: {new_pos} @ {new_avg}")
        save_strategies_to_disk()
        return True
    else:
        fail_log = f"[ERROR] Execution failed: {error_msg}"
        strategy["status"] = fail_log
        log_message("ERROR", f"Strategy Trade Failed: {fail_log}")
        save_strategies_to_disk()
        return False

# ==========================================
# 6. LOCAL PERSISTENCE HELPERS
# ==========================================
DATABASE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ticks.db")

def init_db():
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ticks (
                strategy_id TEXT,
                timestamp INTEGER,
                buy_diff REAL,
                sell_diff REAL
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_strategy_ticks ON ticks(strategy_id, timestamp)")
        conn.commit()
        conn.close()
        log_message("SUCCESS", f"SQLite Database initialized successfully at '{DATABASE_PATH}'")
    except Exception as e:
        log_message("ERROR", f"Failed to initialize SQLite Database: {e}")

def get_aggregated_chart_data(strategy_id, timeframe):
    # Map timeframe string to seconds
    tf_map = {
        '1m': 60,
        '5m': 300,
        '10m': 600,
        '15m': 900,
        '30m': 1800,
        '1h': 3600,
        '1d': 86400
    }
    interval = tf_map.get(timeframe, 60)
    
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT timestamp, buy_diff, sell_diff 
        FROM ticks 
        WHERE strategy_id = ? 
        ORDER BY timestamp ASC
    """, (strategy_id,))
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        return []
    
    # Group ticks by interval
    grouped = {}
    for timestamp, buy_diff, sell_diff in rows:
        group_time = (timestamp // interval) * interval
        if group_time not in grouped:
            grouped[group_time] = []
        grouped[group_time].append((timestamp, buy_diff, sell_diff))
        
    chart_data = []
    for g_time in sorted(grouped.keys()):
        group_ticks = grouped[g_time]
        group_ticks.sort(key=lambda x: x[0])
        
        buy_diffs = [x[1] for x in group_ticks]
        sell_diffs = [x[2] for x in group_ticks]
        
        chart_data.append({
            "time": g_time,
            "buy": {
                "open": buy_diffs[0],
                "high": max(buy_diffs),
                "low": min(buy_diffs),
                "close": buy_diffs[-1]
            },
            "sell": {
                "open": sell_diffs[0],
                "high": max(sell_diffs),
                "low": min(sell_diffs),
                "close": sell_diffs[-1]
            },
            "buy_avg": round(sum(buy_diffs) / len(buy_diffs), 2),
            "sell_avg": round(sum(sell_diffs) / len(sell_diffs), 2)
        })
        
    return chart_data

def save_strategies_to_disk():
    try:
        with open(state.STRATEGIES_FILE, "w") as f:
            json.dump(state.active_strategies, f, indent=4)
    except Exception as e:
        print(f"[ERROR] Failed to save strategies to file: {e}")

def load_strategies_from_disk():
    pass # global replaced
    if os.path.exists(state.STRATEGIES_FILE):
        try:
            with open(state.STRATEGIES_FILE, "r") as f:
                state.active_strategies = json.load(f)
            # Ensure strategy_type and position tracking fields are present on loaded strategies
            for strat in state.active_strategies.values():
                if "strategy_type" not in strat:
                    strat["strategy_type"] = "SPREAD"
                if "position" not in strat:
                    strat["position"] = 0.0
                if "avg_entry_diff" not in strat:
                    strat["avg_entry_diff"] = None
                if "required_margin" not in strat:
                    strat["required_margin"] = None
                if "pnl" not in strat:
                    strat["pnl"] = 0.0
            log_message("SUCCESS", f"Loaded {len(state.active_strategies)} strategies from '{state.STRATEGIES_FILE}'")
        except Exception as e:
            log_message("ERROR", f"Failed to load '{state.STRATEGIES_FILE}': {e}. Starting fresh.")
            state.active_strategies = {}
    else:
        state.active_strategies = {}

def seed_sample_strategies():
    """
    Seeds a couple of demo strategies dynamically using currently valid expiries so the user
    sees pricing active out-of-the-box.
    """
    pass # global replaced
    if state.active_strategies:
        return
        
    log_message("INFO", "Seeding template multi-leg strategies...")
    
    # 1. DLF CE Spread (NFO)
    nfo_expiry = lookup_engine.sorted_nfo_expiries[0] if lookup_engine.sorted_nfo_expiries else "25-Jun-2026"
    norm_nfo = normalize_expiry(nfo_expiry)
    
    leg1 = lookup_engine.lookup("DLF", norm_nfo, 800, "CE")
    leg2 = lookup_engine.lookup("DLF", norm_nfo, 820, "CE")
    
    if leg1 and leg2:
        state.active_strategies["1001"] = {
            "symbol": "DLF",
            "expiry": nfo_expiry,
            "strategy_type": "SPREAD",
            "strategy_lot": 1.0,
            "target_buy": 5.0,
            "target_sell": -15.0,
            "trade_action": "SET TARGET BUY",
            "execute_trigger": "",
            "buy_diff": None,
            "sell_diff": None,
            "est_cost": None,
            "cost_per_share": None,
            "required_margin": None,
            "position": 0.0,
            "avg_entry_diff": None,
            "pnl": 0.0,
            "status": "Strategy active.",
            "legs": [
                {
                    "strike": 800,
                    "opt_type": "CE",
                    "expiry": nfo_expiry,
                    "action": "BUY",
                    "lot": 1.0,
                    "token": leg1["token"],
                    "symbol": leg1["symbol"],
                    "lotsize": leg1["lotsize"],
                    "exch_seg": "NFO",
                    "status": "Pending live quotes..."
                },
                {
                    "strike": 820,
                    "opt_type": "CE",
                    "expiry": nfo_expiry,
                    "action": "SELL",
                    "lot": 1.0,
                    "token": leg2["token"],
                    "symbol": leg2["symbol"],
                    "lotsize": leg2["lotsize"],
                    "exch_seg": "NFO",
                    "status": "Pending live quotes..."
                }
            ]
        }
        
    # 2. GOLD Option Spread (MCX)
    mcx_expiry = lookup_engine.sorted_mcx_expiries[0] if lookup_engine.sorted_mcx_expiries else "26-May-2026"
    norm_mcx = normalize_expiry(mcx_expiry)
    
    mleg1 = lookup_engine.lookup("GOLD", norm_mcx, 72000, "CE")
    mleg2 = lookup_engine.lookup("GOLD", norm_mcx, 73000, "CE")
    
    if mleg1 and mleg2:
        state.active_strategies["1002"] = {
            "symbol": "GOLD",
            "expiry": mcx_expiry,
            "strategy_type": "SPREAD",
            "strategy_lot": 1.0,
            "target_buy": 100.0,
            "target_sell": -200.0,
            "trade_action": "",
            "execute_trigger": "",
            "buy_diff": None,
            "sell_diff": None,
            "est_cost": None,
            "cost_per_share": None,
            "required_margin": None,
            "position": 0.0,
            "avg_entry_diff": None,
            "pnl": 0.0,
            "status": "Strategy active.",
            "legs": [
                {
                    "strike": 72000,
                    "opt_type": "CE",
                    "expiry": mcx_expiry,
                    "action": "BUY",
                    "lot": 1.0,
                    "token": mleg1["token"],
                    "symbol": mleg1["symbol"],
                    "lotsize": mleg1["lotsize"],
                    "exch_seg": "MCX",
                    "status": "Pending live quotes..."
                },
                {
                    "strike": 73000,
                    "opt_type": "CE",
                    "expiry": mcx_expiry,
                    "action": "SELL",
                    "lot": 1.0,
                    "token": mleg2["token"],
                    "symbol": mleg2["symbol"],
                    "lotsize": mleg2["lotsize"],
                    "exch_seg": "MCX",
                    "status": "Pending live quotes..."
                }
            ]
        }
        
    save_strategies_to_disk()
    log_message("SUCCESS", "Template strategies successfully seeded on first launch!")

def calculate_depth_price(depth_list, target_qty, fallback_price):
    if not depth_list or target_qty <= 0:
        return fallback_price
    
    remaining_qty = target_qty
    total_value = 0.0
    
    for level in depth_list:
        price = float(level.get("price", 0.0))
        qty = int(level.get("quantity", 0))
        
        if qty <= 0:
            continue
            
        if remaining_qty <= qty:
            total_value += remaining_qty * price
            remaining_qty = 0
            break
        else:
            total_value += qty * price
            remaining_qty -= qty
            
    # If there's still remaining quantity (not enough depth), fill the rest at the last level's price
    if remaining_qty > 0:
        last_price = float(depth_list[-1].get("price", fallback_price))
        total_value += remaining_qty * last_price
        
    return total_value / target_qty

# ==========================================
# 7. TRADING ENGINE BACKGROUND LOOP
# ==========================================

def run_trading_engine_thread():
    log_message("INFO", "Starting Multi-Tenant Trade Hub trading loop background process...")
    while True:
        try:
            if not state.engine_running:
                time.sleep(1.0)
                continue
            # Iterate over all registered multi-tenant sessions
            active_sessions = list(state.sessions.keys())
            if not active_sessions:
                time.sleep(1.0)
                continue
                
            for cc in active_sessions:
                client_ctx.set(cc)
                process_trading_tick()
            time.sleep(0.1)
        except Exception as e:
            log_message("ERROR", f"Engine loop error: {e}")
            time.sleep(1.0)

def process_trading_tick():
    
    
    if True:
        try:
            # 1. Self-healing token check and verification
            if not state.unified_broker.connected:
                config = load_client_config()
                if config:
                    creds = config.get("credentials", {})
                    if creds.get("totp_seed") or os.path.exists(state.TOKENS_FILE):
                        log_message("WARNING", "Reconnecting session to active Broker API...")
                        state.unified_broker.connect(config)
                if not state.unified_broker.connected:
                    time.sleep(10.0)
                    return
                    
            # 2. Gather active tokens
            tokens_to_fetch = {} # Mapping token -> exch_seg
            
            with state_lock:
                for strat in state.active_strategies.values():
                    for leg in strat["legs"]:
                        if leg.get("token"): # ONLY fetch if token is resolved
                            tokens_to_fetch[leg["token"]] = leg.get("exch_seg", "NFO")
            
            # 3. Pull real-time quotes in grouped chunks
            market_data = {}
            if tokens_to_fetch:
                by_exchange = {}
                for token, exch in tokens_to_fetch.items():
                    if exch not in by_exchange:
                        by_exchange[exch] = []
                    by_exchange[exch].append(token)
                    
                chunk_size = 45
                try:
                    for exch, tokens in by_exchange.items():
                        for i in range(0, len(tokens), chunk_size):
                            chunk = tokens[i:i+chunk_size]
                            chunk_data = state.unified_broker.get_market_data({exch: chunk})
                            market_data.update(chunk_data)
                            is_ws_active = bool(getattr(state.unified_broker, "_angel_ticker", None) or getattr(state.unified_broker, "_zerodha_ticker", None) or getattr(state.unified_broker, "_fyers_ticker", None))
                            if not is_ws_active:
                                time.sleep(0.4)  # Cooldown only for pure REST API polling
                except Exception as e:
                    err_msg = str(e)
                    if "exceeding access rate" in err_msg.lower() or "access denied" in err_msg.lower() or "too many requests" in err_msg.lower():
                        log_message("WARNING", f"Rate limit exceeded (Access Rate Exceeded). Sleeping for 5s to cool down...")
                        time.sleep(5.0)
                    elif any(x in err_msg.lower() for x in ["timeout", "timed out", "connection", "connect", "max retries exceeded", "host", "pool", "disconnected", "remote end"]):
                        log_message("WARNING", f"Feed API Network Timeout/Error: {e}. Retrying in 3s...")
                        time.sleep(3.0)
                    else:
                        log_message("WARNING", f"Feed API Exception: {e}. Authenticating a fresh token...")
                        config = load_client_config()
                        if config:
                            state.unified_broker.connect(config)
                        time.sleep(2.0)
                    return
            
            # 4. Perform Badla Spread Calculations & Evaluate Triggers
            ticks_to_save = []
            with state_lock:
                for strat_id, strat in list(state.active_strategies.items()):
                    legs = strat["legs"]
                    if not legs:
                        strat["buy_diff"] = None
                        strat["sell_diff"] = None
                        strat["est_cost"] = None
                        strat["cost_per_share"] = None
                        strat["status"] = "Configure leg strikes below."
                        continue
                        
                    unconfigured_legs = [leg for leg in legs if not leg.get("token")]
                    if unconfigured_legs:
                        strat["buy_diff"] = None
                        strat["sell_diff"] = None
                        strat["est_cost"] = None
                        strat["cost_per_share"] = None
                        strat["status"] = "Configure all leg strikes to stream spreads."
                        for leg in legs:
                            if not leg.get("token"):
                                leg["status"] = leg.get("status") or "Strike not configured."
                        continue
                        
                    missing_tokens = [leg for leg in legs if leg.get("token") and leg["token"] not in market_data]
                    if missing_tokens:
                        strat["status"] = "Waiting for live quotes..."
                        for leg in legs:
                            if leg.get("token") and leg["token"] not in market_data:
                                leg["status"] = "Waiting for live quotes..."
                        continue
                        
                    # 1. Update quotes and VWAP for all legs
                    for leg in legs:
                        token = leg["token"]
                        q = market_data[token]
                        ltp, bid, ask = q["ltp"], q["bid"], q["ask"]
                        
                        leg["ltp"] = ltp
                        
                        lotsize = leg.get("lotsize", 1)
                        strategy_mult = strat.get("strategy_lot", 1.0)
                        leg_qty = int(lotsize * leg["lot"] * strategy_mult)
                        leg["qty"] = leg_qty
                        
                        depth_bid = calculate_depth_price(q.get("buy_depth", []), leg_qty, bid)
                        depth_ask = calculate_depth_price(q.get("sell_depth", []), leg_qty, ask)
                        
                        leg["bid"] = round(depth_bid, 2)
                        leg["ask"] = round(depth_ask, 2)
                        
                        leg["status"] = "Live update active."

                    # 2. Compute buy_diff and sell_diff based on strategy type
                    if strat.get("strategy_type") == "DELTA" and len(legs) == 2:
                        leg_stock = legs[0]
                        leg_opt = legs[1]
                        
                        bid_stock = leg_stock["bid"]
                        ask_stock = leg_stock["ask"]
                        
                        bid_opt = leg_opt["bid"]
                        ask_opt = leg_opt["ask"]
                        
                        lot_opt = leg_opt["lot"]
                        
                        if leg_opt["action"] == "SELL":
                            # Option is SOLD (e.g., covered call)
                            buy_diff = ask_stock - (lot_opt * bid_opt)
                            sell_diff = bid_stock - (lot_opt * ask_opt)
                        else:
                            # Option is BOUGHT (e.g., protective put)
                            buy_diff = ask_stock + (lot_opt * ask_opt)
                            sell_diff = bid_stock + (lot_opt * bid_opt)
                    else:
                        # Standard SPREAD math
                        buy_diff = 0.0
                        sell_diff = 0.0
                        for leg in legs:
                            lot = leg["lot"]
                            depth_bid = leg["bid"]
                            depth_ask = leg["ask"]
                            if leg["action"] == "BUY":
                                buy_diff += lot * depth_ask
                                sell_diff += lot * depth_bid
                            elif leg["action"] == "SELL":
                                buy_diff -= lot * depth_bid
                                sell_diff -= lot * depth_ask
                                
                    strat["buy_diff"] = round(buy_diff, 2)
                    strat["sell_diff"] = round(sell_diff, 2)
                    
                    # Calculate estimated round-trip transaction cost ("kharcha") for both sides
                    total_est_cost = 0.0
                    for leg in legs:
                        leg_ltp = leg.get("ltp")
                        if leg_ltp is not None:
                            leg_qty = leg.get("qty", 0)
                            # 2-sided turnover (buying and selling combined)
                            leg_turnover = 2.0 * float(leg_ltp) * float(leg_qty)
                            opt_type = leg.get("opt_type", "").upper()
                            if opt_type in ("CE", "PE"):
                                rate = 15000.0 / 10000000.0  # 15000 per crore
                            else:
                                rate = 2000.0 / 10000000.0   # 2000 per crore
                            total_est_cost += leg_turnover * rate
                    strat["est_cost"] = round(total_est_cost, 2)
                    
                    # Cost per unit of strategy lot size (comparable to spread difference)
                    base_lotsize = 1
                    if legs:
                        base_lotsize = legs[0].get("lotsize", 1)
                    if base_lotsize <= 0:
                        base_lotsize = 1
                        
                    strategy_mult = strat.get("strategy_lot", 1.0)
                    if strategy_mult <= 0:
                        strategy_mult = 1.0
                        
                    cost_per_share = total_est_cost / (base_lotsize * strategy_mult)
                    strat["cost_per_share"] = round(cost_per_share, 4)

                    # Calculate required margin for this strategy
                    try:
                        strat["required_margin"] = state.unified_broker.calculate_strategy_margin(legs, strategy_mult)
                    except Exception as e:
                        log_message("WARNING", f"Error calculating margin: {e}")
                        strat["required_margin"] = None
                        
                    # Calculate live P&L for the strategy position
                    pnl = 0.0
                    position = strat.get("position", 0.0) or 0.0
                    avg_entry_diff = strat.get("avg_entry_diff")
                    if position != 0.0 and avg_entry_diff is not None and legs:
                        current_val = 0.0
                        for leg in legs:
                            sign = 1.0 if leg.get("action") == "BUY" else -1.0
                            lot = float(leg.get("lot", 1.0) or 1.0)
                            ltp = float(leg.get("ltp") or 0.0)
                            current_val += sign * lot * ltp
                        
                        base_lotsize = float(legs[0].get("lotsize", 1.0) or 1.0)
                        if base_lotsize <= 0:
                            base_lotsize = 1.0
                        
                        pnl = (current_val - avg_entry_diff) * position * base_lotsize
                        pnl = round(pnl, 2)
                    strat["pnl"] = pnl
                    
                    # Record strategy spread ticks for historical charting
                    if strat.get("buy_diff") is not None and strat.get("sell_diff") is not None:
                        ticks_to_save.append((strat_id, int(time.time()), strat["buy_diff"], strat["sell_diff"]))
                    
                    # 5. Check order triggers
                    if not state.engine_running:
                        strat["status"] = "[ENGINE STOPPED] Trade engine is paused."
                        return
                        
                    trade_action = strat.get("trade_action", "")
                    execute_trigger = strat.get("execute_trigger", "")
                    target_buy = strat.get("target_buy")
                    target_sell = strat.get("target_sell")
                    rounded_buy = strat["buy_diff"]
                    rounded_sell = strat["sell_diff"]
                    
                    if trade_action == "BUY NOW":
                        if execute_trigger != "GO":
                            strat["status"] = "[CONFIRM REQUIRED] Click GO to trigger BUY."
                        else:
                            execute_strategy_trade(state.unified_broker, strat, "BUY")
                            
                    elif trade_action == "SELL NOW":
                        if execute_trigger != "GO":
                            strat["status"] = "[CONFIRM REQUIRED] Click GO to trigger SELL."
                        else:
                            execute_strategy_trade(state.unified_broker, strat, "SELL")
                            
                    elif trade_action == "SET TARGET BUY":
                        if target_buy is None:
                            strat["status"] = "[ERROR] Enter Target Buy parameter."
                        else:
                            if execute_trigger != "GO":
                                strat["status"] = f"[WAITING] Activate with GO | Target Buy: {target_buy}"
                            else:
                                if rounded_buy <= target_buy:
                                    log_message("SUCCESS", f"[TRIGGER] {strat['symbol']} Buy Diff {rounded_buy} <= Target {target_buy}!")
                                    execute_strategy_trade(state.unified_broker, strat, "BUY")
                                else:
                                    strat["status"] = f"[ACTIVE] Target Buy: {target_buy} | Current Buy: {rounded_buy} | Waiting..."
                                    
                    elif trade_action == "SET TARGET SELL":
                        if target_sell is None:
                            strat["status"] = "[ERROR] Enter Target Sell parameter."
                        else:
                            if execute_trigger != "GO":
                                strat["status"] = f"[WAITING] Activate with GO | Target Sell: {target_sell}"
                            else:
                                if rounded_sell >= target_sell:
                                    log_message("SUCCESS", f"[TRIGGER] {strat['symbol']} Sell Diff {rounded_sell} >= Target {target_sell}!")
                                    execute_strategy_trade(state.unified_broker, strat, "SELL")
                                else:
                                    strat["status"] = f"[ACTIVE] Target Sell: {target_sell} | Current Sell: {rounded_sell} | Waiting..."
                                    
                    elif trade_action == "CANCEL":
                        strat["target_buy"] = null = None
                        strat["target_sell"] = null = None
                        strat["trade_action"] = ""
                        strat["execute_trigger"] = ""
                        strat["status"] = "Trigger cancelled."
                        save_strategies_to_disk()
                        
                    else:
                        strat["status"] = "Live update active."
            
            # Save recorded ticks to database asynchronously outside the state lock
            if ticks_to_save:
                def _async_save_ticks(t_list):
                    try:
                        conn = sqlite3.connect(DATABASE_PATH)
                        cursor = conn.cursor()
                        cursor.executemany("INSERT INTO ticks (strategy_id, timestamp, buy_diff, sell_diff) VALUES (?, ?, ?, ?)", t_list)
                        conn.commit()
                        conn.close()
                    except Exception as db_err:
                        log_message("WARNING", f"Failed to save ticks to database: {db_err}")
                threading.Thread(target=_async_save_ticks, args=(ticks_to_save,), daemon=True).start()
            
            time.sleep(0.05)
            
        except Exception as e:
            log_message("ERROR", f"Trading Engine loop caught exception: {e}")
            time.sleep(3.0)

USERS_FILE = "users.json"

def hash_password(pwd):
    return hashlib.sha256(pwd.encode('utf-8')).hexdigest()

def load_users():
    if not os.path.exists(USERS_FILE):
        default_users = {
            "admin": {
                "password_hash": hash_password("admin123"),
                "role": "admin"
            }
        }
        try:
            with open(USERS_FILE, 'w') as f:
                json.dump(default_users, f, indent=4)
            log_message("INFO", "Created default users.json with admin credentials (admin / admin123).")
        except Exception as e:
            log_message("ERROR", f"Failed to write default users.json: {e}")
        return default_users
    try:
        with open(USERS_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        log_message("ERROR", f"Failed to load users.json: {e}")
        return {}

def verify_user(username, password):
    users = load_users()
    u = users.get(username)
    if u and u.get("password_hash") == hash_password(password):
        return True, u.get("role", "user")
    return False, None

# ==========================================
# 8. FLASK SERVER ENDPOINTS
# ==========================================
app = Flask(__name__, template_folder='templates')

@app.before_request
def set_tenant_context():
    cc = request.headers.get('X-Client-Code', 'DEFAULT')
    client_ctx.set(cc)

app.secret_key = os.environ.get("FLASK_SECRET_KEY", "trade_hub_pro_secret_key_998877")

_engine_initialized = False

def init_app_engine():
    global _engine_initialized, lookup_engine
    if _engine_initialized:
        return
    _engine_initialized = True
    print("\n" + "="*70)
    print("  COMMERCIAL MULTI-BROKER OPTIONS TRADE HUB ENGINE RUNNING (ONLINE PROD DASHBOARD)")
    print("="*70 + "\n")
    log_message("INFO", "Initializing Trade Hub Pro Engine components...")
    migrate_client_data_files()
    init_db()
    state.unified_broker = UnifiedBrokerClient()
    lookup_engine = ScripMasterLookup("OpenAPIScripMaster.json")
    load_strategies_from_disk()
    if not state.active_strategies:
        seed_sample_strategies()
    trading_thread = threading.Thread(target=run_trading_engine_thread, daemon=True)
    trading_thread.start()
    log_message("SUCCESS", "Trade Hub Engine background loop started.")

# Auto-initialize when loaded by Gunicorn or Flask
init_app_engine()


@app.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        res = app.make_default_options_response()
        res.headers['Access-Control-Allow-Origin'] = '*'
        res.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Client-Code, X-Admin-Token'
        res.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
        return res, 200

@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Client-Code, X-Admin-Token'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    return response

@app.route('/')
def home():
    if os.path.exists('index.html'):
        return send_file('index.html')
    return send_from_directory('templates', 'index.html')

# Registration OTP caching dictionary
temp_otp_cache = {}


def logout_helper():
    pass # global replaced
    state.engine_running = False
    log_message("WARNING", "Clearing SaaS broker credentials...")
    
    if os.path.exists(state.CLIENT_CONFIG_FILE):
        try:
            os.remove(state.CLIENT_CONFIG_FILE)
        except Exception:
            pass
            
    if os.path.exists(state.TOKENS_FILE):
        try:
            os.remove(state.TOKENS_FILE)
        except Exception:
            pass
            
    # Clean disconnect broker connection (closes WebSockets, clears caches)
    state.unified_broker.disconnect()

def panic_stop_helper():
    pass # global replaced
    state.engine_running = False
    log_message("CRITICAL", "[EMERGENCY STOP] All strategies have been cancelled and triggers deactivated!")
    with state_lock:
        for strat in state.active_strategies.values():
            strat["target_buy"] = None
            strat["target_sell"] = None
            strat["trade_action"] = ""
            strat["execute_trigger"] = ""
            strat["status"] = "[EMERGENCY STOPPED] Strategy cancelled."
            for leg in strat["legs"]:
                leg["status"] = "Emergency cancelled."
        save_strategies_to_disk()

@app.route('/api/auth/status', methods=['GET'])
def auth_status():
    has_config = os.path.exists(state.CLIENT_CONFIG_FILE)
    return jsonify({
        "registered": has_config,
        "connected": state.unified_broker.connected,
        "profile": state.unified_broker.profile
    })

@app.route('/api/auth/register', methods=['POST'])
def auth_register():
    """Step 3 handler: save credentials and initiate broker first-step authentication.
    For Angel One / Fyers with totp_seed: attempts immediate auto-login.
    Otherwise: validates fields and proceeds to Step 4 TOTP/OTP entry.
    """
    global temp_otp_cache
    try:
        req = request.json
        if not req:
            return jsonify({"status": "error", "message": "Invalid request body. Expected JSON."}), 400

        name = req.get("client_name", "").strip()
        email = req.get("client_email", "").strip()
        mobile = req.get("client_mobile", "").strip()
        broker = req.get("selected_broker", "ANGEL_ONE").upper().strip()
        creds = req.get("credentials", {}) or {}

        if not name or not email or not mobile:
            return jsonify({"status": "error", "message": "Name, Email, and Mobile are required."}), 400

        tenant_cc = request.headers.get('X-Client-Code') or creds.get('client_code') or mobile
        client_ctx.set(tenant_cc)
        state.get_session(tenant_cc)

        broker_std = "ANGEL_ONE"
        if "ZERODHA" in broker:
            broker_std = "ZERODHA"
        elif "FYERS" in broker:
            broker_std = "FYERS"
        elif "GROWW" in broker:
            broker_std = "GROWW"

        # Validate that required broker-specific fields are not empty
        api_key = creds.get("api_key", "").strip()
        client_code = creds.get("client_code", "").strip()
        password = creds.get("password", "").strip()

        if not api_key or not client_code or not password:
            missing = []
            if not api_key: missing.append("API Key / App ID")
            if not client_code: missing.append("Client ID / User ID")
            if not password: missing.append("MPIN / Password")
            return jsonify({"status": "error", "message": f"Missing required fields: {', '.join(missing)}"}), 400

        step4_hint = "Enter your 6-digit TOTP code from your authenticator app."

        # For Angel One: auto-connect if totp_seed is provided
        if broker_std == "ANGEL_ONE":
            totp_seed = creds.get("totp_seed", "").strip()
            if totp_seed:
                log_message("INFO", f"Angel One TOTP Key provided. Attempting automatic login for {client_code}...")
                try:
                    temp_cache = {
                        "client_name": name,
                        "client_email": email,
                        "client_mobile": mobile,
                        "selected_broker": "ANGEL_ONE",
                        "credentials": creds
                    }
                    success = state.unified_broker.connect(temp_cache)
                    if success and state.unified_broker.connected:
                        config_data = {
                            "client_name": name,
                            "client_email": email,
                            "client_mobile": mobile,
                            "selected_broker": "ANGEL_ONE",
                            "credentials": temp_cache["credentials"],
                            "mode": "REAL",
                            "registered_at": datetime.now().isoformat()
                        }
                        save_client_config(config_data)
                        log_message("SUCCESS", f"Client '{name}' successfully auto-connected to Angel One in REAL mode.")
                        return jsonify({
                            "status": "success",
                            "connected": True,
                            "message": "Successfully connected to Angel One automatically using your TOTP key!"
                        })
                    else:
                        err_detail = state.unified_broker.profile.get("error_details", "Auto-connection failed.")
                        log_message("WARNING", f"Angel One auto-connection with TOTP seed failed: {err_detail}. Proceeding to manual 2FA card.")
                        step4_hint = f"Angel One auto-login failed: {err_detail}. Please enter your 6-digit Authenticator TOTP manually below."
                except Exception as e:
                    log_message("WARNING", f"Angel One auto-connection exception: {e}. Proceeding to manual 2FA card.")
                    step4_hint = f"Angel One auto-login error: {str(e)}. Please enter your 6-digit Authenticator TOTP manually below."

        # For Fyers: initiate send_login_otp to trigger phone OTP or do TOTP auto-login
        if broker_std == "FYERS":
            totp_seed = creds.get("totp_seed", "").strip()
            if totp_seed:
                log_message("INFO", f"Fyers TOTP Key provided. Attempting automatic login for {client_code}...")
                try:
                    # 1. Trigger the OTP request to get a request_key
                    r = state.unified_broker._fyers_post_request(
                        "/vagator/v2/send_login_otp",
                        json_payload={"fy_id": client_code, "app_id": "2"},
                        timeout=10
                    )
                    d = r.json()
                    if d.get("s") != "ok" or not d.get("request_key"):
                        err = d.get("message") or d.get("error") or "Fyers rejected Client ID."
                        return jsonify({"status": "error", "message": f"Fyers login initiation failed: {err}"}), 400
                    
                    # 2. Store the request key in credentials
                    creds["fyers_request_key"] = d["request_key"]
                    creds["fyers_totp_enabled"] = d.get("totp_enabled", True)
                    
                    # 3. Setup temp_otp_cache structure so connect() method works perfectly
                    temp_cache = {
                        "client_name": name,
                        "client_email": email,
                        "client_mobile": mobile,
                        "selected_broker": "FYERS",
                        "credentials": creds
                    }
                    
                    # 4. Call connect() which will automatically generate TOTP, verify MPIN, and exchange token!
                    success = state.unified_broker.connect(temp_cache)
                    if success and state.unified_broker.connected:
                        config_data = {
                            "client_name": name,
                            "client_email": email,
                            "client_mobile": mobile,
                            "selected_broker": "FYERS",
                            "credentials": temp_cache["credentials"],
                            "mode": "REAL",
                            "registered_at": datetime.now().isoformat()
                        }
                        save_client_config(config_data)
                        log_message("SUCCESS", f"Client '{name}' successfully auto-connected to FYERS in REAL mode.")
                        return jsonify({
                            "status": "success", 
                            "connected": True, 
                            "message": "Successfully connected to FYERS automatically using your TOTP key!"
                        })
                    else:
                        err_detail = state.unified_broker.profile.get("error_details", "Auto-connection failed.")
                        log_message("WARNING", f"Fyers auto-connection with TOTP seed failed: {err_detail}. Falling back to manual OTP card.")
                        step4_hint = f"Fyers auto-login failed: {err_detail}. Please enter your 6-digit SMS OTP or current Authenticator TOTP manually below."
                except Exception as e:
                    log_message("WARNING", f"Fyers auto-connection exception: {e}. Falling back to manual OTP card.")
                    step4_hint = f"Fyers auto-login failed with error: {str(e)}. Please enter your 6-digit SMS OTP or current Authenticator TOTP manually below."
            
            # Normal Fyers login flow without totp_seed (prompts for Step 4 OTP)
            try:
                log_message("INFO", f"Fyers Step 1: Sending login OTP to {client_code}...")
                r = state.unified_broker._fyers_post_request(
                    "/vagator/v2/send_login_otp",
                    json_payload={"fy_id": client_code, "app_id": "2"},
                    timeout=10
                )
                d = r.json()
                log_message("INFO", f"Fyers send_login_otp response: s={d.get('s')} code={d.get('code')} msg={d.get('message')}")
                if d.get("s") != "ok" or not d.get("request_key"):
                    err = d.get("message") or d.get("error") or "Fyers rejected your Client ID. Please verify it."
                    log_message("ERROR", f"Fyers send_login_otp failed: {d}")
                    return jsonify({"status": "error", "message": f"Fyers login initiation failed: {err}"}), 400

                fyers_req_key = d["request_key"]
                totp_enabled = d.get("totp_enabled", False)
                creds["fyers_request_key"] = fyers_req_key
                creds["fyers_totp_enabled"] = totp_enabled

                step4_hint = "Fyers sent an SMS OTP to your registered phone. Enter that 6-digit SMS OTP below. If that is rejected, try entering the 6-digit TOTP from your authenticator app (Google Authenticator)."
                log_message("INFO", f"Fyers send_login_otp successfully initiated. totp_enabled={totp_enabled}")
            except json.JSONDecodeError:
                log_message("ERROR", "Fyers send_login_otp returned a non-JSON HTML block (often due to temporary IP rate limits from too many fast requests).")
                return jsonify({"status": "error", "message": "Fyers server is temporarily rate-limiting connection requests. Please wait 5 minutes and try again."}), 429
            except requests.exceptions.ConnectionError:
                return jsonify({"status": "error", "message": "Could not reach Fyers servers. Please check your internet connection."}), 503
            except Exception as e:
                log_message("ERROR", f"Fyers send_login_otp exception: {e}")
                return jsonify({"status": "error", "message": f"Fyers connection error: {str(e)}"}), 500

        temp_otp_cache = {
            "client_name": name,
            "client_email": email,
            "client_mobile": mobile,
            "selected_broker": broker_std,
            "credentials": creds
        }

        log_message("INFO", f"Registration Step 3 complete for {name} ({broker_std}). Proceeding to Step 4 authentication.")
        return jsonify({"status": "success", "message": step4_hint})

    except Exception as ex:
        log_message("ERROR", f"Unhandled exception in auth_register: {ex}")
        import traceback; traceback.print_exc()
        return jsonify({"status": "error", "message": f"Server error: {str(ex)}"}), 500

@app.route('/api/auth/verify-otp', methods=['POST'])
def auth_verify_otp():
    """Step 4 handler: completes broker authentication with OTP/TOTP.
    NO demo bypass, NO mock login. If credentials are wrong, returns a clear error.
    """
    global temp_otp_cache
    req = request.json
    otp_entered = str(req.get("otp", "")).strip()

    if not temp_otp_cache:
        return jsonify({"status": "error", "message": "Registration session expired. Please go back and start from Step 1."}), 400

    if not otp_entered:
        return jsonify({"status": "error", "message": "Please enter your OTP / TOTP code to continue."}), 400

    # Store the OTP/TOTP in credentials so the connection methods can use it
    temp_otp_cache["credentials"]["entered_otp"] = otp_entered
    broker_name = temp_otp_cache.get("selected_broker", "BROKER")

    log_message("INFO", f"Step 4: Attempting real {broker_name} authentication...")

    # Attempt REAL broker authentication - no bypass, no fallback
    success = state.unified_broker.connect(temp_otp_cache)

    if not success:
        err_detail = state.unified_broker.profile.get("error_details",
            "Authentication failed. Please verify all your credentials and try again.")
        log_message("ERROR", f"{broker_name} authentication failed: {err_detail}")
        return jsonify({
            "status": "error",
            "message": f"❌ {broker_name} authentication failed: {err_detail}"
        }), 401

    # Save only if truly connected
    if not state.unified_broker.connected:
        return jsonify({"status": "error", "message": "Broker reported success but connection state is invalid. Please retry."}), 500

    config_data = {
        "client_name": temp_otp_cache["client_name"],
        "client_email": temp_otp_cache["client_email"],
        "client_mobile": temp_otp_cache["client_mobile"],
        "selected_broker": broker_name,
        "credentials": temp_otp_cache["credentials"],
        "mode": "REAL",
        "registered_at": datetime.now().isoformat()
    }

    save_client_config(config_data)
    temp_otp_cache = {}

    log_message("SUCCESS", f"Client '{config_data['client_name']}' successfully connected to {broker_name} in REAL mode.")
    return jsonify({"status": "success", "message": f"Successfully connected to {broker_name}!"})

@app.route('/api/auth/logout', methods=['POST'])
def auth_logout():
    logout_helper()
    panic_stop_helper()
    return jsonify({"status": "success"})

@app.route('/api/state', methods=['GET'])
def get_state():
    # Fetch funds outside the state lock to prevent blocking/lock contention.
    # Positions and orders fetching disabled to prevent hanging/performance issues.
    funds = state.unified_broker.get_funds()
    
    n_ltp, n_chg, n_pct = get_nifty_live_price()
    
    # Load client registration config details
    config = load_client_config() or {}
    email = config.get("client_email", "--")
    mobile = config.get("client_mobile", "--")
    reg_at = config.get("registered_at", "--")
    if reg_at and reg_at != "--":
        try:
            # Format Iso date string to DD/MM/YYYY
            if "T" in reg_at:
                reg_at = reg_at.split("T")[0]
            parts = reg_at.split("-")
            if len(parts) == 3:
                reg_at = f"{parts[2]}/{parts[1]}/{parts[0]}"
        except Exception:
            pass
            
    with state_lock:
        state_dict = {
            "profile_name": state.unified_broker.profile["name"],
            "client_code": state.unified_broker.profile["client_code"],
            "broker": state.unified_broker.profile["broker"],
            "connected": state.unified_broker.connected,
            "mode": state.unified_broker.mode,
            "strategies": state.active_strategies,
            "logs": state.app_logs,
            "nfo_expiries": lookup_engine.sorted_nfo_expiries if lookup_engine else [],
            "mcx_expiries": lookup_engine.sorted_mcx_expiries if lookup_engine else [],
            "nfo_symbols": lookup_engine.nfo_symbols if lookup_engine else [],
            "mcx_symbols": lookup_engine.mcx_symbols if lookup_engine else [],
            "engine_running": state.engine_running,
            "funds": funds,
            "positions": [],
            "orders": [],
            "nifty_ltp": n_ltp,
            "nifty_change": n_chg,
            "nifty_pct": n_pct,
            "client_email": email,
            "client_mobile": mobile,
            "registered_at": reg_at
        }
    return jsonify(state_dict)

@app.route('/api/engine/start', methods=['POST'])
def start_engine():
    pass # global replaced
    with state_lock:
        state.engine_running = True
    log_message("SUCCESS", "Trading Engine STARTED. Live monitoring and execution is active.")
    return jsonify({"status": "success"})

@app.route('/api/engine/stop', methods=['POST'])
def stop_engine():
    pass # global replaced
    with state_lock:
        state.engine_running = False
    log_message("WARNING", "Trading Engine STOPPED.")
    return jsonify({"status": "success"})

@app.route('/api/engine/panic-stop', methods=['POST'])
def panic_stop():
    panic_stop_helper()
    return jsonify({"status": "success"})

@app.route('/api/strategy/add', methods=['POST'])
def add_strategy():
    pass # global replaced
    req = request.json
    
    symbol = req.get("symbol", "").upper().strip()
    expiry = req.get("expiry", "").strip()
    strategy_type = req.get("strategy_type", "SPREAD").upper().strip()
    strategy_lot = float(req.get("strategy_lot", 1.0))
    target_buy = req.get("target_buy")
    target_sell = req.get("target_sell")
    exch_seg = req.get("exch_seg", "NFO").upper().strip()
    legs_count = int(req.get("legs_count", 2))
    req_legs = req.get("legs", [])
    
    if target_buy is not None: target_buy = float(target_buy)
    if target_sell is not None: target_sell = float(target_sell)
    
    if not symbol or not expiry:
        return jsonify({"status": "error", "message": "Missing required parameters (Symbol, Expiry)"}), 400
        
    legs = []
    if not req_legs:
        if strategy_type == "DELTA":
            underlying = lookup_engine.lookup_underlying(symbol, expiry)
            if underlying:
                legs.append({
                    "strike": "UNDERLYING",
                    "opt_type": "FUT" if underlying["exch_seg"] in ("NFO", "MCX") else "STOCK",
                    "expiry": expiry if underlying["exch_seg"] != "NSE" else "",
                    "action": "BUY",
                    "lot": 1.0,
                    "token": underlying["token"],
                    "symbol": underlying["symbol"],
                    "lotsize": underlying["lotsize"],
                    "exch_seg": underlying["exch_seg"],
                    "status": "Pending live quotes..."
                })
            else:
                legs.append({
                    "strike": "UNDERLYING",
                    "opt_type": "STOCK",
                    "expiry": "",
                    "action": "BUY",
                    "lot": 1.0,
                    "token": "",
                    "symbol": symbol + "-EQ",
                    "lotsize": 1,
                    "exch_seg": "NSE",
                    "status": "Underlying not found in Scrip Master!"
                })
                
            legs.append({
                "strike": "",
                "opt_type": "CE",
                "expiry": expiry,
                "action": "SELL",
                "lot": 1.0,
                "token": "",
                "symbol": "",
                "lotsize": 1,
                "exch_seg": exch_seg,
                "status": "Strike not configured."
            })
        else:
            # Create N default blank legs
            for i in range(legs_count):
                legs.append({
                    "strike": "",
                    "opt_type": "CE",
                    "expiry": expiry,
                    "action": "BUY" if i % 2 == 0 else "SELL",
                    "lot": 1.0,
                    "token": "",
                    "symbol": "",
                    "lotsize": 1,
                    "exch_seg": exch_seg,
                    "status": "Strike not configured."
                })
    else:
        # Legacy/Modal structured legs parsing
        for rl in req_legs:
            strike_raw = rl.get("strike")
            opt_type = rl.get("opt_type", "CE").upper().strip()
            leg_expiry = rl.get("expiry") or expiry
            action = rl.get("action", "BUY").upper().strip()
            lot = float(rl.get("lot", 1.0))
            
            if strike_raw == "UNDERLYING" or opt_type in ("STOCK", "FUT"):
                if opt_type == "STOCK":
                    contract = lookup_engine.lookup_underlying(symbol)
                else:
                    contract = lookup_engine.lookup_underlying(symbol, leg_expiry)
                
                if contract:
                    legs.append({
                        "strike": "UNDERLYING",
                        "opt_type": opt_type,
                        "expiry": leg_expiry if opt_type == "FUT" else "",
                        "action": action,
                        "lot": lot,
                        "token": contract["token"],
                        "symbol": contract["symbol"],
                        "lotsize": contract["lotsize"],
                        "exch_seg": contract["exch_seg"],
                        "status": "Pending live quotes..."
                    })
                else:
                    legs.append({
                        "strike": "UNDERLYING",
                        "opt_type": opt_type,
                        "expiry": leg_expiry if opt_type == "FUT" else "",
                        "action": action,
                        "lot": lot,
                        "token": "",
                        "symbol": symbol + "-EQ" if opt_type == "STOCK" else symbol + "FUT",
                        "lotsize": 1,
                        "exch_seg": "NSE" if opt_type == "STOCK" else "NFO",
                        "status": "Underlying not found in Scrip Master!"
                    })
                continue
                
            try:
                strike = int(round(float(strike_raw)))
            except Exception:
                return jsonify({"status": "error", "message": f"Invalid leg strike value: '{strike_raw}'"}), 400
                
            norm_expiry = normalize_expiry(leg_expiry)
            
            contract = lookup_engine.lookup(symbol, norm_expiry, strike, opt_type)
            if not contract:
                return jsonify({
                    "status": "error", 
                    "message": f"Contract for {symbol} {leg_expiry} {strike} {opt_type} not found in Scrip Master!"
                }), 400
                
            legs.append({
                "strike": str(strike),
                "opt_type": opt_type,
                "expiry": leg_expiry,
                "action": action,
                "lot": lot,
                "token": contract["token"],
                "symbol": contract["symbol"],
                "lotsize": contract["lotsize"],
                "exch_seg": contract.get("exch_seg", "NFO"),
                "status": "Pending live quotes..."
            })
        
    # Generate unique header ID
    with state_lock:
        new_id = str(max([int(k) for k in state.active_strategies.keys()] + [1000]) + 1)
        state.active_strategies[new_id] = {
            "symbol": symbol,
            "expiry": expiry,
            "strategy_type": strategy_type,
            "strategy_lot": strategy_lot,
            "target_buy": target_buy,
            "target_sell": target_sell,
            "trade_action": "",
            "execute_trigger": "",
            "buy_diff": None,
            "sell_diff": None,
            "est_cost": None,
            "cost_per_share": None,
            "required_margin": None,
            "position": 0.0,
            "avg_entry_diff": None,
            "pnl": 0.0,
            "status": "Strategy active.",
            "legs": legs
        }
        save_strategies_to_disk()
        
    log_message("SUCCESS", f"Added new spread strategy {symbol} ({expiry}) with {len(legs)} legs. Assigned ID: {new_id}")
    return jsonify({"status": "success", "id": new_id})

@app.route('/api/strategy/update', methods=['POST'])
def update_strategy():
    pass # global replaced
    req = request.json
    
    h_row = str(req.get("header_row"))
    field = req.get("field")
    value = req.get("value")
    
    with state_lock:
        if h_row not in state.active_strategies:
            return jsonify({"status": "error", "message": "Strategy ID not found"}), 404
            
        strat = state.active_strategies[h_row]
        
        if field == "target_buy":
            strat["target_buy"] = float(value) if (value is not None and str(value).strip() != "") else None
        elif field == "target_sell":
            strat["target_sell"] = float(value) if (value is not None and str(value).strip() != "") else None
        elif field == "strategy_lot":
            strat["strategy_lot"] = float(value) if (value is not None and str(value).strip() != "") else 1.0
        elif field == "trade_action":
            strat["trade_action"] = str(value).strip().upper()
        elif field == "execute_trigger":
            strat["execute_trigger"] = str(value).strip().upper()
            
        save_strategies_to_disk()
        
    return jsonify({"status": "success"})

@app.route('/api/strategy/leg/update', methods=['POST'])
def update_leg():
    pass # global replaced
    req = request.json
    
    h_row = str(req.get("header_row"))
    leg_idx = int(req.get("leg_index"))
    field = req.get("field")
    value = req.get("value")
    
    with state_lock:
        if h_row not in state.active_strategies:
            return jsonify({"status": "error", "message": "Strategy ID not found"}), 404
            
        strat = state.active_strategies[h_row]
        if leg_idx < 0 or leg_idx >= len(strat["legs"]):
            return jsonify({"status": "error", "message": "Leg index out of range"}), 400
            
        leg = strat["legs"][leg_idx]
        
        if strat.get("strategy_type") == "DELTA" and leg_idx == 0:
            if field == "opt_type":
                leg["opt_type"] = str(value).strip().upper()
            elif field == "expiry":
                leg["expiry"] = str(value).strip()
            elif field == "action":
                leg["action"] = str(value).strip().upper()
            elif field == "lot":
                try:
                    leg["lot"] = float(value)
                except ValueError:
                    leg["lot"] = 1.0
                    
            symbol = strat["symbol"]
            opt_type = leg["opt_type"]
            expiry = leg["expiry"] if opt_type == "FUT" else None
            
            # Lookup contract
            contract = lookup_engine.lookup_underlying(symbol, expiry)
            if contract:
                leg["token"] = contract["token"]
                leg["symbol"] = contract["symbol"]
                leg["lotsize"] = contract["lotsize"]
                leg["exch_seg"] = contract["exch_seg"]
                leg["status"] = "Pending quotes..."
            else:
                leg["token"] = ""
                leg["symbol"] = ""
                leg["status"] = "Underlying contract not found in Scrip Master!"
            save_strategies_to_disk()
            return jsonify({"status": "success"})
        
        if field == "strike":
            leg["strike"] = str(value).strip()
        elif field == "opt_type":
            leg["opt_type"] = str(value).strip().upper()
        elif field == "expiry":
            leg["expiry"] = str(value).strip()
        elif field == "action":
            leg["action"] = str(value).strip().upper()
        elif field == "lot":
            try:
                leg["lot"] = float(value)
            except ValueError:
                leg["lot"] = 1.0
                
        # Perform Scrip Master Lookup if strike is present
        strike_raw = leg.get("strike", "").strip()
        if strike_raw:
            try:
                strike = int(round(float(strike_raw)))
                norm_expiry = normalize_expiry(leg["expiry"])
                symbol = strat["symbol"]
                
                # Perform lookup
                contract = lookup_engine.lookup(symbol, norm_expiry, strike, leg["opt_type"])
                if contract:
                    leg["token"] = contract["token"]
                    leg["symbol"] = contract["symbol"]
                    leg["lotsize"] = contract["lotsize"]
                    leg["exch_seg"] = contract.get("exch_seg", "NFO")
                    leg["status"] = "Pending quotes..."
                else:
                    leg["token"] = ""
                    leg["symbol"] = ""
                    leg["lotsize"] = 1
                    leg["status"] = f"Contract not found in Scrip Master!"
            except Exception as e:
                leg["token"] = ""
                leg["symbol"] = ""
                leg["status"] = f"Error: {str(e)}"
        else:
            leg["token"] = ""
            leg["symbol"] = ""
            leg["status"] = "Strike not configured."
            
        save_strategies_to_disk()
        
    return jsonify({"status": "success"})

@app.route('/api/strategy/leg/add', methods=['POST'])
def add_leg():
    pass # global replaced
    req = request.json
    
    h_row = str(req.get("header_row"))
    
    with state_lock:
        if h_row not in state.active_strategies:
            return jsonify({"status": "error", "message": "Strategy ID not found"}), 404
            
        strat = state.active_strategies[h_row]
        exch_seg = "NFO"
        if strat["legs"]:
            exch_seg = strat["legs"][0].get("exch_seg", "NFO")
            
        strat["legs"].append({
            "strike": "",
            "opt_type": "CE",
            "expiry": strat["expiry"],
            "action": "BUY" if len(strat["legs"]) % 2 == 0 else "SELL",
            "lot": 1.0,
            "token": "",
            "symbol": "",
            "lotsize": 1,
            "exch_seg": exch_seg,
            "status": "Strike not configured."
        })
        save_strategies_to_disk()
        
    return jsonify({"status": "success"})

@app.route('/api/strategy/leg/delete', methods=['POST'])
def delete_leg():
    pass # global replaced
    req = request.json
    
    h_row = str(req.get("header_row"))
    leg_idx = int(req.get("leg_index"))
    
    with state_lock:
        if h_row not in state.active_strategies:
            return jsonify({"status": "error", "message": "Strategy ID not found"}), 404
            
        strat = state.active_strategies[h_row]
        if leg_idx < 0 or leg_idx >= len(strat["legs"]):
            return jsonify({"status": "error", "message": "Leg index out of range"}), 400
            
        strat["legs"].pop(leg_idx)
        save_strategies_to_disk()
        
    return jsonify({"status": "success"})

@app.route('/api/strategy/delete', methods=['POST'])
def delete_strategy():
    pass # global replaced
    req = request.json
    h_row = str(req.get("header_row"))
    
    with state_lock:
        if h_row in state.active_strategies:
            symbol = state.active_strategies[h_row]["symbol"]
            del state.active_strategies[h_row]
            save_strategies_to_disk()
            log_message("SUCCESS", f"Deleted Strategy ID: {h_row} ({symbol})")
            
            # Clean up tick history for deleted strategy
            try:
                conn = sqlite3.connect(DATABASE_PATH)
                cursor = conn.cursor()
                cursor.execute("DELETE FROM ticks WHERE strategy_id = ?", (h_row,))
                conn.commit()
                conn.close()
            except Exception as e:
                log_message("WARNING", f"Failed to delete ticks for strategy {h_row}: {e}")
                
            return jsonify({"status": "success"})
        else:
            return jsonify({"status": "error", "message": "Strategy ID not found"}), 404

def aggregate_spread_candles(candles_list, target_tf):
    if not candles_list:
        return []
    if target_tf != '10m':
        return candles_list
        
    interval = 600 # 10 minutes in seconds
    grouped = {}
    for c in candles_list:
        g_time = (c["time"] // interval) * interval
        if g_time not in grouped:
            grouped[g_time] = []
        grouped[g_time].append(c)
        
    aggregated = []
    for g_time in sorted(grouped.keys()):
        group = grouped[g_time]
        group.sort(key=lambda x: x["time"])
        
        buy_opens = group[0]["buy"]["open"]
        buy_closes = group[-1]["buy"]["close"]
        buy_highs = max(x["buy"]["high"] for x in group)
        buy_lows = min(x["buy"]["low"] for x in group)
        
        sell_opens = group[0]["sell"]["open"]
        sell_closes = group[-1]["sell"]["close"]
        sell_highs = max(x["sell"]["high"] for x in group)
        sell_lows = min(x["sell"]["low"] for x in group)
        
        vol = sum(x.get("volume", 0) for x in group)
        
        aggregated.append({
            "time": g_time,
            "buy": {
                "open": buy_opens,
                "high": buy_highs,
                "low": buy_lows,
                "close": buy_closes
            },
            "sell": {
                "open": sell_opens,
                "high": sell_highs,
                "low": sell_lows,
                "close": sell_closes
            },
            "buy_avg": round((buy_highs + buy_lows) / 2, 2),
            "sell_avg": round((sell_highs + sell_lows) / 2, 2),
            "volume": vol
        })
    return aggregated

@app.route('/api/strategy/chart', methods=['GET'])
def get_strategy_chart_data_api():
    strategy_id = request.args.get("id")
    timeframe = request.args.get("timeframe", "1m")
    if not strategy_id:
        return jsonify({"status": "error", "message": "Missing strategy id"}), 400
        
    with state_lock:
        if strategy_id not in state.active_strategies:
            return jsonify({"status": "error", "message": "Strategy not found"}), 404
        strat = state.active_strategies[strategy_id]
        legs = list(strat.get("legs", []))
        
    # Check if we can fetch from the broker
    if state.unified_broker.connected and legs and all(leg.get("token") for leg in legs):
        try:
            from datetime import timedelta
            now = datetime.now()
            to_date = now
            
            # Request expanded historical query ranges (to fetch much more back date data)
            if timeframe == '1m':
                from_date = now - timedelta(days=30)
            elif timeframe == '5m' or timeframe == '10m':
                from_date = now - timedelta(days=60)
            elif timeframe == '15m' or timeframe == '30m':
                from_date = now - timedelta(days=90)
            elif timeframe == '1h':
                from_date = now - timedelta(days=180)
            else: # 1d
                from_date = now - timedelta(days=730)
                
            broker_timeframe = timeframe
            if timeframe == '10m':
                broker_timeframe = '5m' # Fetch 5m candles and aggregate in Python
                
            leg_histories = []
            for leg in legs:
                token = leg.get("token")
                exch_seg = leg.get("exch_seg", "NFO")
                symbol = leg.get("symbol")
                lot = float(leg.get("lot", 1.0) or 1.0)
                action = leg.get("action", "BUY").upper()
                sign = 1.0 if action == "BUY" else -1.0
                
                # Fetch leg candles from broker
                candles = state.unified_broker.get_historical_candles(exch_seg, token, symbol, broker_timeframe, from_date, to_date)
                if not candles:
                    # If any leg has no history, raise to fall back to SQLite
                    raise Exception(f"No historical candles returned for leg {symbol}")
                    
                candles_dict = {c["time"]: c for c in candles}
                leg_histories.append({
                    "sign": sign,
                    "lot": lot,
                    "candles": candles_dict,
                    "raw_list": candles
                })
                
            # Find common timestamps
            all_timestamps = set()
            for lh in leg_histories:
                all_timestamps.update(lh["candles"].keys())
            sorted_timestamps = sorted(list(all_timestamps))
            
            # Forward-fill state tracker
            last_closes = {}
            last_volumes = {}
            for idx, lh in enumerate(leg_histories):
                # Default to first available close/volume or 0
                last_closes[idx] = lh["raw_list"][0]["close"] if lh["raw_list"] else 0.0
                last_volumes[idx] = lh["raw_list"][0].get("volume", 0) if lh["raw_list"] else 0
                
            chart_data = []
            for t in sorted_timestamps:
                # Update last seen close prices and volumes for legs that have a candle at this timestamp
                for idx, lh in enumerate(leg_histories):
                    if t in lh["candles"]:
                        last_closes[idx] = lh["candles"][t]["close"]
                        last_volumes[idx] = lh["candles"][t].get("volume", 0)
                        
                buy_open = 0.0
                buy_close = 0.0
                buy_high = 0.0
                buy_low = 0.0
                
                sell_open = 0.0
                sell_close = 0.0
                sell_high = 0.0
                sell_low = 0.0
                
                volume_sum = 0
                
                for idx, lh in enumerate(leg_histories):
                    sign = lh["sign"]
                    lot = lh["lot"]
                    
                    if t in lh["candles"]:
                        c = lh["candles"][t]
                        o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
                        vol = c.get("volume", 0)
                    else:
                        # Forward-filled values
                        o = h = l = cl = last_closes[idx]
                        vol = last_volumes[idx]
                        
                    # Calculate Buy Spread (using leg prices)
                    buy_open += sign * lot * o
                    buy_close += sign * lot * cl
                    buy_high += lot * (h if sign > 0.0 else -l)
                    buy_low += lot * (l if sign > 0.0 else -h)
                    
                    # Calculate Sell Spread (approximated same as buy spread for historical data)
                    sell_open += sign * lot * o
                    sell_close += sign * lot * cl
                    sell_high += lot * (h if sign > 0.0 else -l)
                    sell_low += lot * (l if sign > 0.0 else -h)
                    
                    volume_sum += vol
                    
                chart_data.append({
                    "time": t,
                    "buy": {
                        "open": round(buy_open, 2),
                        "high": round(buy_high, 2),
                        "low": round(buy_low, 2),
                        "close": round(buy_close, 2)
                    },
                    "sell": {
                        "open": round(sell_open, 2),
                        "high": round(sell_high, 2),
                        "low": round(sell_low, 2),
                        "close": round(sell_close, 2)
                    },
                    "buy_avg": round((buy_high + buy_low) / 2, 2),
                    "sell_avg": round((sell_high + sell_low) / 2, 2),
                    "volume": volume_sum
                })
                
            # Perform aggregation if target timeframe is 10m
            if timeframe == '10m':
                chart_data = aggregate_spread_candles(chart_data, '10m')
                
            return jsonify({"status": "success", "data": chart_data, "source": "broker"})
        except Exception as broker_err:
            log_message("WARNING", f"Historical broker chart fetch failed for {strategy_id}, falling back to local database: {broker_err}")
            
    # Fallback to local SQLite tick database
    try:
        data = get_aggregated_chart_data(strategy_id, timeframe)
        return jsonify({"status": "success", "data": data, "source": "local_db"})
    except Exception as e:
        log_message("ERROR", f"Error fetching chart data from database: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/strategy/position/detect', methods=['POST'])
def detect_strategy_position():
    pass # global replaced
    req = request.json
    h_row = str(req.get("header_row"))
    
    with state_lock:
        if h_row not in state.active_strategies:
            return jsonify({"status": "error", "message": "Strategy ID not found"}), 404
            
        strat = state.active_strategies[h_row]
        if not state.unified_broker.connected:
            return jsonify({"status": "error", "message": "Broker not connected"}), 400
            
        try:
            broker_positions = state.unified_broker.get_positions()
            if not broker_positions:
                return jsonify({"status": "success", "position": 0.0, "avg_entry_diff": None, "message": "No active positions found in broker account."})
                
            matched_positions = []
            for p in broker_positions:
                qty = 0.0
                avg_price = 0.0
                if state.unified_broker.broker == "ANGEL_ONE":
                    qty = safe_float(p.get("netqty"))
                    avg_price = safe_float(p.get("netprice") or p.get("avgprice"))
                elif state.unified_broker.broker == "ZERODHA":
                    qty = safe_float(p.get("quantity"))
                    avg_price = safe_float(p.get("average_price"))
                elif state.unified_broker.broker == "FYERS":
                    qty = safe_float(p.get("netQty"))
                    avg_price = safe_float(p.get("avgPrice"))
                    
                if abs(qty) < 0.01:
                    continue
                    
                contract = lookup_engine.find_contract_by_position(p, state.unified_broker.broker)
                if contract:
                    c_name = contract.get("name", "").upper()
                    c_expiry = contract.get("expiry", "")
                    c_opt_type = contract.get("opt_type", "STOCK")
                    
                    # Match symbol (underlying name, e.g. NIFTY, DLF)
                    if c_name != strat.get("symbol", "").upper():
                        continue
                        
                    # Match expiry (for options/futures)
                    if c_opt_type != "STOCK":
                        if normalize_expiry(c_expiry) != normalize_expiry(strat.get("expiry")):
                            continue
                            
                    matched_positions.append({
                        "contract": contract,
                        "qty": qty,
                        "avg_price": avg_price
                    })
                    
            if not matched_positions:
                return jsonify({
                    "status": "success",
                    "position": 0.0,
                    "avg_entry_diff": None,
                    "message": f"No active broker positions found for {strat.get('symbol')} ({strat.get('expiry')})."
                })
                
            # Rebuild strategy legs
            new_legs = []
            lots_list = []
            
            for item in matched_positions:
                contract = item["contract"]
                qty = item["qty"]
                avg_price = item["avg_price"]
                lotsize = float(contract.get("lotsize", 1.0) or 1.0)
                if lotsize <= 0:
                    lotsize = 1.0
                
                lots = abs(qty) / lotsize
                lots_list.append(lots)
                
                # Determine leg action:
                # If quantity > 0, it's BUY. If quantity < 0, it's SELL.
                leg_action = "BUY" if qty > 0 else "SELL"
                
                # Determine strike display (none for STOCK/FUT)
                strike_val = contract.get("strike")
                if contract.get("opt_type") in ("CE", "PE"):
                    try:
                        strike_val = float(strike_val) if "." in str(strike_val) else int(strike_val)
                    except Exception:
                        pass
                else:
                    strike_val = ""
                    
                new_legs.append({
                    "strike": strike_val,
                    "opt_type": contract.get("opt_type", "STOCK"),
                    "expiry": contract.get("expiry") or strat.get("expiry"),
                    "action": leg_action,
                    "lot": lots,  # Temporary absolute lot value, will scale below
                    "token": contract.get("token"),
                    "symbol": contract.get("symbol"),
                    "lotsize": lotsize,
                    "exch_seg": contract.get("exch_seg") or "NFO",
                    "status": "Live update active.",
                    "entry_price": avg_price
                })
                
            # Find the base scaling factor (min lots)
            min_lots = min(lots_list)
            if min_lots <= 0.001:
                min_lots = 1.0
                
            # Rescale each leg relative to min_lots
            for leg in new_legs:
                raw_lots = leg["lot"]
                leg["lot"] = round(raw_lots / min_lots, 2)
                
            # Calculate avg_entry_diff
            total_diff = 0.0
            for leg in new_legs:
                sign = 1.0 if leg["action"] == "BUY" else -1.0
                total_diff += sign * leg["lot"] * leg["entry_price"]
                
            detected_pos = round(min_lots, 2)
            detected_avg = round(total_diff, 2)
            
            # Save rebuilt legs to the strategy
            strat["legs"] = new_legs
            strat["position"] = detected_pos
            strat["avg_entry_diff"] = detected_avg
            save_strategies_to_disk()
            
            return jsonify({
                "status": "success",
                "position": detected_pos,
                "avg_entry_diff": detected_avg,
                "message": f"Successfully synced {len(new_legs)} legs from broker. Position: {detected_pos} @ {detected_avg}"
            })
            
        except Exception as e:
            return jsonify({"status": "error", "message": f"Error during detection: {str(e)}"}), 500

@app.route('/api/strategy/position/update', methods=['POST'])
def update_strategy_position_api():
    pass # global replaced
    req = request.json
    h_row = str(req.get("header_row"))
    try:
        position = float(req.get("position", 0.0))
        avg_val = req.get("avg_entry_diff")
        avg_entry_diff = float(avg_val) if avg_val is not None and str(avg_val).strip() != "" else None
    except (ValueError, TypeError):
        return jsonify({"status": "error", "message": "Invalid numeric values"}), 400
        
    with state_lock:
        if h_row not in state.active_strategies:
            return jsonify({"status": "error", "message": "Strategy ID not found"}), 404
            
        strat = state.active_strategies[h_row]
        strat["position"] = round(position, 2)
        strat["avg_entry_diff"] = round(avg_entry_diff, 2) if avg_entry_diff is not None else None
        
        save_strategies_to_disk()
        log_message("SUCCESS", f"Strategy {strat['symbol']} position manually set to {position} @ {avg_entry_diff}")
        
    return jsonify({"status": "success"})

@app.route('/api/strategy/import-all', methods=['POST'])
def import_all_strategies_from_broker():
    pass # global replaced
    if not state.unified_broker.connected:
        return jsonify({"status": "error", "message": "Broker not connected"}), 400
        
    try:
        with state_lock:
            load_strategies_from_disk()
        broker_positions = state.unified_broker.get_positions()
        if not broker_positions:
            return jsonify({"status": "success", "imported_count": 0, "message": "No active positions found in broker account."})
            
        # Group active positions by underlying name
        groups = {}
        for p in broker_positions:
            qty = 0.0
            avg_price = 0.0
            if state.unified_broker.broker == "ANGEL_ONE":
                qty = safe_float(p.get("netqty"))
                avg_price = safe_float(p.get("netprice") or p.get("avgprice"))
            elif state.unified_broker.broker == "ZERODHA":
                qty = safe_float(p.get("quantity"))
                avg_price = safe_float(p.get("average_price"))
            elif state.unified_broker.broker == "FYERS":
                qty = safe_float(p.get("netQty"))
                avg_price = safe_float(p.get("avgPrice"))
                
            if abs(qty) < 0.01:
                continue
                
            contract = lookup_engine.find_contract_by_position(p, state.unified_broker.broker)
            if contract:
                underlying = contract.get("name", "").upper()
                
                # Group by underlying name (e.g. "SILVERM", "CRUDEOIL", "WIPRO")
                group_key = underlying
                if group_key not in groups:
                    groups[group_key] = []
                    
                groups[group_key].append({
                    "contract": contract,
                    "qty": qty,
                    "avg_price": avg_price
                })
                
        if not groups:
            return jsonify({"status": "success", "imported_count": 0, "message": "No active NFO/MCX/NSE positions could be matched."})
            
        imported_count = 0
        with state_lock:
            for group_key, items in groups.items():
                underlying = group_key
                
                # Determine display expiry
                display_expiry = ""
                for it in items:
                    if it["contract"].get("expiry"):
                        display_expiry = it["contract"].get("expiry")
                        break
                        
                # Rebuild legs
                new_legs = []
                lots_list = []
                for item in items:
                    contract = item["contract"]
                    qty = item["qty"]
                    avg_price = item["avg_price"]
                    lotsize = float(contract.get("lotsize", 1.0) or 1.0)
                    if lotsize <= 0:
                        lotsize = 1.0
                        
                    lots = abs(qty) / lotsize
                    lots_list.append(lots)
                    
                    leg_action = "BUY" if qty > 0 else "SELL"
                    
                    strike_val = contract.get("strike")
                    if contract.get("opt_type") in ("CE", "PE"):
                        try:
                            strike_val = float(strike_val) if "." in str(strike_val) else int(strike_val)
                        except Exception:
                            pass
                    else:
                        strike_val = ""
                        
                    new_legs.append({
                        "strike": strike_val,
                        "opt_type": contract.get("opt_type", "STOCK"),
                        "expiry": contract.get("expiry") or display_expiry,
                        "action": leg_action,
                        "lot": lots,  # Temporary
                        "token": contract.get("token"),
                        "symbol": contract.get("symbol"),
                        "lotsize": lotsize,
                        "exch_seg": contract.get("exch_seg") or "NFO",
                        "status": "Live update active.",
                        "entry_price": avg_price
                    })
                    
                min_lots = min(lots_list)
                if min_lots <= 0.001:
                    min_lots = 1.0
                    
                for leg in new_legs:
                    raw_lots = leg["lot"]
                    leg["lot"] = round(raw_lots / min_lots, 2)
                    
                total_diff = 0.0
                for leg in new_legs:
                    sign = 1.0 if leg["action"] == "BUY" else -1.0
                    total_diff += sign * leg["lot"] * leg["entry_price"]
                    
                detected_pos = round(min_lots, 2)
                detected_avg = round(total_diff, 2)
                
                # Check for existing strategy match (matching only by symbol/underlying)
                matched_strat_id = None
                for strat_id, strat in state.active_strategies.items():
                    if strat.get("symbol", "").upper() == underlying:
                        matched_strat_id = strat_id
                        break
                        
                if matched_strat_id:
                    # Update existing strategy
                    strat = state.active_strategies[matched_strat_id]
                    strat["legs"] = new_legs
                    strat["position"] = detected_pos
                    strat["avg_entry_diff"] = detected_avg
                    strat["expiry"] = display_expiry or strat.get("expiry")
                    strat["status"] = "Imported/Synced from broker."
                else:
                    # Create new strategy
                    new_id = str(max([int(k) for k in state.active_strategies.keys()] + [1000]) + 1)
                    state.active_strategies[new_id] = {
                        "symbol": underlying,
                        "expiry": display_expiry,
                        "strategy_type": "SPREAD",
                        "strategy_lot": 1.0,
                        "target_buy": None,
                        "target_sell": None,
                        "trade_action": "",
                        "execute_trigger": "",
                        "buy_diff": None,
                        "sell_diff": None,
                        "est_cost": None,
                        "cost_per_share": None,
                        "required_margin": None,
                        "position": detected_pos,
                        "avg_entry_diff": detected_avg,
                        "pnl": 0.0,
                        "status": "Imported from broker.",
                        "legs": new_legs
                    }
                imported_count += 1
                
            save_strategies_to_disk()
            
        return jsonify({
            "status": "success",
            "imported_count": imported_count,
            "message": f"Successfully imported/synced {imported_count} strategies from broker."
        })
        
    except Exception as e:
        return jsonify({"status": "error", "message": f"Import failed: {str(e)}"}), 500

@app.route('/api/logs/clear', methods=['POST'])
def clear_logs():
    pass # global replaced
    with state_lock:
        state.app_logs = []
    return jsonify({"status": "success"})

# ==========================================
# 8.5. GEMINI AI CHATBOT ROUTE
# ==========================================
GEMINI_API_KEY = "AQ.Ab8RN6Jmq0cLch_htAJao5L7qZNAKs9bULT1N5dHaDi-8bOM6g"

@app.route('/api/chat', methods=['POST'])
def chat_api():
    try:
        data = request.json
        user_message = data.get("message", "").strip()
        if not user_message:
            return jsonify({"status": "error", "message": "Message is empty"}), 400
        
        # Read strategy status details for extra context
        with state_lock:
            num_strategies = len(state.active_strategies)
            strat_symbols = [strat["symbol"] for strat in state.active_strategies.values()]
        
        # Detailed system prompt describing the Trade Hub app, options, and business guide
        system_context = (
            "You are Trade Hub AI Assistant, a helpful options trading and technical assistant integrated "
            "directly into the Trade Hub application.\n"
            "Here is the context about Trade Hub:\n"
            "- Trade Hub is an options trading dashboard that automates, monitors, and executes multi-leg strategies "
            "(e.g., spreads, deltas, straddles, strangles, butterflies).\n"
            "- Supported Brokers: Angel One, Fyers, and Groww. Groww is partially supported, Zerodha is supported under the hood.\n"
            "- Active Strategies currently configured: " + str(num_strategies) + " strategies. Symbols: " + ", ".join(strat_symbols) + ".\n"
            "- Option Spreads (called 'Badla' in Hindi/Gujarati trading terminology) refer to the price difference "
            "between legs (e.g., buying one leg and selling another). The system displays 'Buy Diff' and 'Sell Diff'.\n"
            "- Trading Engine: There is a background thread that monitors market data. When the engine is started, "
            "it monitors target buy/sell differences and triggers order execution automatically.\n"
            "- Manual GO Execution: The engine has a self-directed execution model where the user manually clicks "
            "execution, which makes it safe from strict SEBI Investment Advisory regulations.\n"
            "- Commercialization & SaaS Roadmap:\n"
            "  * Tech Architecture: To scale Trade Hub, transition to multi-tenant DB (PostgreSQL/Supabase), "
            "encrypt API credentials using AES-256, and use Celery/Redis for background jobs.\n"
            "  * SEBI Regulations: Market it as an analytical calculator, not an advisory AI, so SEBI registration is not needed.\n"
            "  * Pricing: Standard subscription plan (Index Options) is ₹999/month, Professional Plan (Pro Options & Commodity) is ₹2,499/month. "
            "For payment, Razorpay (UPI auto-pay) is recommended in India, Stripe globally.\n"
            "  * How to Sell: Sell subscription SaaS to Telegram/YouTube trading communities, or sell the intellectual property (source code) "
            "on Acquire.com or Flippa for upfront cash (₹5 Lakh to ₹25 Lakh).\n\n"
            "Guidelines for response:\n"
            "1. Answer in the same language the user asks (supports Hinglish, Hindi, and English).\n"
            "2. Keep the responses concise, accurate, and structured with bullet points where appropriate.\n"
            "3. If a question is unrelated to options trading, brokers, finance, or Trade Hub, politely redirect the user back to Trade Hub topics."
        )
        
        payload = {
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {"text": system_context + "\n\nUser Question: " + user_message}
                    ]
                }
            ],
            "generationConfig": {
                "temperature": 0.7,
                "topP": 0.95,
                "maxOutputTokens": 1024
            }
        }
        
        headers = {
            "Content-Type": "application/json"
        }
        
        api_key = os.environ.get("GEMINI_API_KEY", GEMINI_API_KEY)
        
        # Loop through fallback candidate models to handle high demand or timeouts
        candidate_models = ["gemini-2.5-flash-lite", "gemini-3.1-flash-lite", "gemini-2.5-flash", "gemini-2.0-flash", "gemini-flash-latest"]
        last_error = "No models attempted"
        
        for model in candidate_models:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
            log_message("INFO", f"Attempting Gemini request with model: {model}")
            try:
                # Use a larger timeout of 20 seconds to give the API extra time under high load
                response = requests.post(url, json=payload, headers=headers, timeout=20)
                
                # Check status code
                if response.status_code == 200:
                    response_data = response.json()
                    try:
                        reply = response_data["candidates"][0]["content"]["parts"][0]["text"]
                        log_message("SUCCESS", f"Successfully completed chatbot query with model: {model}")
                        return jsonify({"status": "success", "reply": reply})
                    except (KeyError, IndexError) as parse_err:
                        last_error = f"Failed to parse response for model {model}: {parse_err}"
                        log_message("WARNING", last_error)
                        continue
                else:
                    try:
                        err_json = response.json()
                        err_msg = err_json.get("error", {}).get("message", f"Status code {response.status_code}")
                    except Exception:
                        err_msg = f"Status code {response.status_code}"
                    
                    last_error = f"Model {model} failed (Status {response.status_code}): {err_msg}"
                    log_message("WARNING", last_error)
                    continue
            except requests.exceptions.Timeout:
                last_error = f"Model {model} request timed out (20s limit reached)."
                log_message("WARNING", last_error)
                continue
            except Exception as ex:
                last_error = f"Model {model} encountered error: {str(ex)}"
                log_message("WARNING", last_error)
                continue
                
        # If all models failed, return the last error message
        log_message("ERROR", f"All Gemini models failed. Last error details: {last_error}")
        return jsonify({"status": "error", "message": f"Gemini API is currently busy. Please try again. (Details: {last_error})"}), 503
        
    except Exception as e:
        log_message("ERROR", f"Error in chatbot API: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# ==========================================
# 9. MAIN RUNNER & AUTO-LAUNCHER
# ==========================================
# 9. MAIN RUNNER & AUTO-LAUNCHER
# ==========================================
# ==========================================
# ADMIN DASHBOARD ROUTES
# ==========================================
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "123"

@app.route('/admin')
def admin_dashboard():
    return render_template('admin.html')

@app.route('/api/admin/login', methods=['POST'])
def admin_login():
    data = request.get_json()
    if data and data.get('username') == ADMIN_USERNAME and data.get('password') == ADMIN_PASSWORD:
        return jsonify({"status": "success", "token": "admin_master_token_777"})
    return jsonify({"status": "error", "message": "Invalid credentials"}), 401

@app.route('/api/admin/data', methods=['GET'])
def admin_data():
    if request.headers.get('X-Admin-Token') != "admin_master_token_777":
        return jsonify({"status": "error", "message": "Unauthorized"}), 401
        
    # Scan disk for all client configs and load sessions
    import glob
    cfg_files = glob.glob(os.path.join(CLIENT_DATA_DIR, "client_config_*.json")) + glob.glob("client_config_*.json")
    for cfg_file in cfg_files:
        cc_match = re.search(r'client_config_(.+)\.json$', cfg_file)
        if cc_match:
            cc = cc_match.group(1)
            state.get_session(cc)
            
    if os.path.exists(get_client_file_path("client_config.json")) or os.path.exists("client_config.json"):
        state.get_session("DEFAULT")

    admin_state = {
        "clients": [],
        "system_status": "RUNNING" if state.engine_running else "STOPPED",
        "global_logs": state.sessions.get('DEFAULT', {}).get('state.app_logs', [])
    }
    
    for cc, session in state.sessions.items():
        if cc == 'DEFAULT' and len(state.sessions) > 1:
            continue
            
        client_name = "Unknown"
        client_mobile = "Unknown"
        client_broker = "Unknown"
        
        cfg_file = get_client_file_path(f"client_config_{cc}.json" if cc != 'DEFAULT' else "client_config.json")
        if not os.path.exists(cfg_file):
            cfg_file = f"client_config_{cc}.json" if cc != 'DEFAULT' else "client_config.json"
        if os.path.exists(cfg_file):
            try:
                with open(cfg_file, 'r') as cf:
                    cfg = json.load(cf)
                    client_name = cfg.get('client_name', 'Unknown')
                    client_mobile = cfg.get('client_mobile', 'Unknown')
                    client_broker = cfg.get('selected_broker', 'Unknown')
            except:
                pass
                
        active_strats_dict = session.get('state.active_strategies', {})
        active_strats_count = len(active_strats_dict)
        
        broker = session.get('state.unified_broker')
        broker_status = "Connected" if (broker and broker.connected) else "Disconnected"
        
        pnl = 0.0
        strats_list = []
        for s_id, s in active_strats_dict.items():
            strat_pnl = s.get('overall_pnl', 0.0) or s.get('pnl', 0.0)
            pnl += strat_pnl
            
            legs_formatted = []
            for leg in s.get("legs", []):
                legs_formatted.append({
                    "action": leg.get("action", "BUY"),
                    "opt_type": leg.get("opt_type", "CE"),
                    "strike": leg.get("strike", ""),
                    "expiry": leg.get("expiry", ""),
                    "lot": leg.get("lot", 1.0),
                    "entry_price": leg.get("entry_price", 0.0),
                    "symbol": leg.get("symbol", ""),
                    "status": leg.get("status", "")
                })

            strats_list.append({
                "id": s_id,
                "name": s.get("name", f"Strategy {s.get('symbol', 'NIFTY')}"),
                "symbol": s.get("symbol", "NIFTY"),
                "expiry": s.get("expiry", ""),
                "type": s.get("strategy_type", "SPREAD"),
                "position": s.get("position", 0.0),
                "avg_entry_diff": s.get("avg_entry_diff"),
                "target_buy": s.get("target_buy"),
                "target_sell": s.get("target_sell"),
                "status": s.get("status", "Active"),
                "pnl": round(strat_pnl, 2),
                "legs_count": len(legs_formatted),
                "legs": legs_formatted
            })
            
        admin_state['clients'].append({
            "client_code": cc,
            "name": client_name,
            "mobile": client_mobile,
            "broker_name": client_broker,
            "status": broker_status,
            "strategies_count": active_strats_count,
            "overall_pnl": round(pnl, 2),
            "strategies": strats_list
        })
        
    return jsonify(admin_state)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    print(f"\n[INFO] Starting Flask Backend Server on http://127.0.0.1:{port} ...\n")
    app.run(host='0.0.0.0', port=port, debug=False)
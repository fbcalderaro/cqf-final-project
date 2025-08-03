import configparser
import os
from datetime import datetime, timezone

# --- Create a parser object and read the config file ---
config = configparser.ConfigParser()
config.read('config.ini')

# --- BINANCE Section ---
BINANCE_CONFIG = config['BINANCE']
API_URL = BINANCE_CONFIG.get('api_url')
SYMBOL = BINANCE_CONFIG.get('symbol')
STREAM_INTERVAL = BINANCE_CONFIG.get('stream_interval')
DEFAULT_START_DATE = datetime.strptime(BINANCE_CONFIG.get('default_start_date'), "%Y-%m-%d").replace(tzinfo=timezone.utc)

# --- BINANCE API Section ---
BINANCE_API_CONFIG = config['BINANCE_API']
API_KEY = os.environ.get('BINANCE_KEY_TEST', BINANCE_API_CONFIG.get('api_key'))
API_SECRET = os.environ.get('BINANCE_SECRET_TEST', BINANCE_API_CONFIG.get('api_secret'))


# --- DATABASE Section ---
DB_CONFIG = config['DATABASE']
DB_HOST = os.environ.get('DB_HOST', DB_CONFIG.get('host'))
DB_PORT = os.environ.get('DB_PORT', DB_CONFIG.get('port'))
DB_NAME = os.environ.get('DB_NAME', DB_CONFIG.get('name'))
DB_USER = os.environ.get('DB_USER', DB_CONFIG.get('user'))
DB_PASSWORD = os.environ.get('DB_PASSWORD', DB_CONFIG.get('password'))

# --- STRATEGY Section ---
STRATEGY_CONFIG = config['STRATEGY_ADAPTIVE_TREND_RIDER']
RSI_BUY_ZONE = (STRATEGY_CONFIG.getfloat('rsi_buy_zone_low'), STRATEGY_CONFIG.getfloat('rsi_buy_zone_high'))
RSI_SELL_ZONE = (STRATEGY_CONFIG.getfloat('rsi_sell_zone_low'), STRATEGY_CONFIG.getfloat('rsi_sell_zone_high'))
ADX_THRESHOLD = STRATEGY_CONFIG.getfloat('adx_threshold')

# --- BACKTEST Section ---
BACKTEST_CONFIG = config['BACKTEST']
INITIAL_CAPITAL = BACKTEST_CONFIG.getfloat('initial_capital')
COMMISSION_PCT = BACKTEST_CONFIG.getfloat('commission_pct')
RISK_PER_TRADE_PCT = BACKTEST_CONFIG.getfloat('risk_per_trade_pct')
ATR_STOP_MULTIPLIER = BACKTEST_CONFIG.getfloat('atr_stop_multiplier')
MAX_POSITION_PCT = BACKTEST_CONFIG.getfloat('max_position_pct')
RESAMPLE_INTERVAL = BACKTEST_CONFIG.get('resample_interval')
DATA_LIMIT = BACKTEST_CONFIG.getint('data_limit')
OUTPUT_DIR = BACKTEST_CONFIG.get('output_dir')

# --- DERIVED VALUES ---
SYMBOL_LOWER = SYMBOL.lower()
CANDLES_TABLE_NAME = f"{SYMBOL_LOWER}_{STREAM_INTERVAL}_candles"
INDICATORS_TABLE_NAME = f"{SYMBOL_LOWER}_{STREAM_INTERVAL}_indicators"
SOCKET_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL_LOWER}@kline_{STREAM_INTERVAL}"

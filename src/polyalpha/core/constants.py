import datetime
import zoneinfo

# ── Endpoints ──────────────────────────────────────────────────────────────────

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"
CLOB_WS   = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# ── Timeframes ─────────────────────────────────────────────────────────────────

TIMEFRAME_SECONDS: dict[str, int] = {
    "5m":  300,
    "15m": 900,
    "1h":  3600,
    "4h":  14400,
    "24h": 86400,
}

# ── Assets ─────────────────────────────────────────────────────────────────────

ASSETS: list[str] = ["BTC", "ETH", "SOL", "XRP", "DOGE", "HYPE", "BNB"]

ASSET_NAMES: dict[str, str] = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "XRP": "xrp",
    "DOGE": "doge",
    "HYPE": "hype",
    "BNB": "bnb",
}

TWEET_SUBJECTS: list[str] = ["elon-musk", "white-house", "zelensky"]

# ── Fees ───────────────────────────────────────────────────────────────────────

TAKER_FEE_RATE = 0.02   # 2% simulated taker fee on paper fills

# ── Streaming ──────────────────────────────────────────────────────────────────

WS_PING_INTERVAL  = 10    # seconds — must text-PING before server drops us
WS_PING_TIMEOUT   = 5     # seconds to wait for PONG before marking stale
WS_RETRY_DELAY    = 3.0   # base back-off in seconds (multiplied by attempt #)
WS_MAX_RETRIES    = 10    # attempts before giving up entirely
WS_BACKOFF_FACTOR = 2.0   # exponential backoff multiplier
WS_JITTER         = 0.2   # jitter factor (±20% random variation) to prevent thundering herd

# ── Scraping ─────────────────────────────────────────────────────────────────────

SCRAPE_RECV_TIMEOUT   = 10   # seconds — per-message recv timeout for scraping
SCRAPE_RETRY_ATTEMPTS = 3    # retry attempts before falling back to Binance

# ── Chainlink Streamer ──────────────────────────────────────────────────────────

CL_WS_RECV_TIMEOUT   = 10    # seconds — per-message recv timeout
CL_WS_MAX_RETRIES    = 10    # max reconnection attempts before giving up
CL_WS_BASE_DELAY     = 3.0   # base backoff delay in seconds
CL_WS_BACKOFF_FACTOR = 2.0   # exponential backoff multiplier
CL_WS_JITTER         = 0.2   # jitter factor (±20%) to prevent thundering herd

# ── Data Feed ───────────────────────────────────────────────────────────────────

DEFAULT_CACHE_MAX_TICKS = 1000    # Maximum ticks to keep in WebSocket cache
API_REQUEST_TIMEOUT = 10          # Default timeout for API requests (seconds)
CACHE_EXPIRY_SECONDS = 3600        # Cache expiry time (1 hour)

# ── Sniper Bot ──────────────────────────────────────────────────────────────────

DEFAULT_WINDOW_SECONDS = 35       # Default trading window size
DEFAULT_MAX_CONSECUTIVE_LOSSES = 3
DEFAULT_PRE_WINDOW_BUFFER = 5     # Seconds before window to start monitoring
DEFAULT_POST_WINDOW_TIMEOUT = 10  # Seconds after window to wait for fills
DEFAULT_TA_LOOKBACK_PERIODS = 200 # Default lookback for technical analysis
MARKET_DISCOVERY_BACKOFF = 5      # Seconds to wait after failed market discovery
POSITION_LIMIT_CHECK_DELAY = 10   # Seconds to wait when position limit reached
ROLLOVER_PAUSE = 1                # Seconds to pause between market cycles
STREAM_SETUP_DELAY = 1            # Seconds to wait for stream connection
PRICE_CHECK_INTERVAL = 0.1         # Seconds between price checks
RESOLUTION_TIMEOUT = 120          # Max seconds to wait for market resolution
RESOLUTION_CHECK_INTERVAL = 0.5  # Seconds between resolution checks

# ── Slug helpers ───────────────────────────────────────────────────────────────

def build_slug(asset: str, timeframe: str, window_end_ts: int) -> str:
    """Return the deterministic Gamma event slug for an asset/timeframe window."""
    asset_upper = asset.upper()
    asset_lower = asset.lower()
    full_name = ASSET_NAMES.get(asset_upper, asset_lower)

    if timeframe in ("1h", "24h"):
        dt_utc = datetime.datetime.fromtimestamp(window_end_ts, tz=datetime.timezone.utc)
        tz_et = zoneinfo.ZoneInfo("America/New_York")
        dt_et = dt_utc.astimezone(tz_et)
        
        month_name = dt_et.strftime("%B").lower()
        day = dt_et.day
        year = dt_et.year
        hour_12 = dt_et.strftime("%I").lstrip("0")
        am_pm = dt_et.strftime("%p").lower()
        
        if timeframe == "1h":
            return f"{full_name}-up-or-down-{month_name}-{day}-{year}-{hour_12}{am_pm}-et"
        elif timeframe == "24h":
            return f"what-price-will-{full_name}-hit-on-{month_name}-{day}"
            
    return f"{asset_lower}-updown-{timeframe}-{window_end_ts}"

def build_tweet_slug(subject: str, start_ts: int, end_ts: int | None = None, monthly: bool = False) -> str:
    """Return the Gamma event slug for a tweet market window."""
    dt_start = datetime.datetime.fromtimestamp(start_ts, tz=datetime.timezone.utc)
    try:
        tz_et = zoneinfo.ZoneInfo("America/New_York")
    except Exception:
        tz_et = datetime.timezone(datetime.timedelta(hours=-4))
        
    dt_start_et = dt_start.astimezone(tz_et)
    
    if monthly:
        month_name = dt_start_et.strftime("%B").lower()
        year = dt_start_et.year
        return f"{subject}-of-tweets-{month_name}-{year}"
        
    if end_ts is None:
        raise ValueError("end_ts is required for non-monthly tweet markets")
        
    dt_end = datetime.datetime.fromtimestamp(end_ts, tz=datetime.timezone.utc)
    dt_end_et = dt_end.astimezone(tz_et)
    
    start_month = dt_start_et.strftime("%B").lower()
    start_day = dt_start_et.day
    
    end_month = dt_end_et.strftime("%B").lower()
    end_day = dt_end_et.day
    
    return f"{subject}-of-tweets-{start_month}-{start_day}-{end_month}-{end_day}"

# ── Rate Limiting ───────────────────────────────────────────────────────────────

DEFAULT_RATE_LIMIT_MAX_REQUESTS = 100  # Default max requests per period for rate limiter
DEFAULT_RATE_LIMIT_PERIOD = 1.0  # Default period in seconds for rate limiter

# ── HTTP Configuration ───────────────────────────────────────────────────────────

HTTP_MAX_KEEPALIVE_CONNECTIONS = 20  # Max keepalive connections for HTTP pool
HTTP_MAX_CONNECTIONS = 100  # Max total connections for HTTP pool
HTTP_KEEPALIVE_EXPIRY = 30.0  # Keepalive expiry in seconds
HTTP_RETRY_DELAY_MULTIPLIER = 1.0  # Multiplier for exponential backoff

# ── Market Discovery ───────────────────────────────────────────────────────────

MARKET_CANDIDATE_COUNT = 3  # Number of candidate windows to probe

# ── Price Thresholds ────────────────────────────────────────────────────────────

DEFAULT_PRICE_THRESHOLD = 0.0  # Minimum price change to emit event (0 = any change)
FALLBACK_PRICE = 0.5  # Fallback price when market price is unavailable
PRICE_STALENESS_THRESHOLD = 30  # Seconds before market price is considered stale
MAX_ORDER_PRICE = 0.999  # Cap for slippage-adjusted buy price (Polymarket prices live in (0,1))

# ── Fee Configuration ───────────────────────────────────────────────────────────

FEE_RATE_SPORTS = 0.03  # Fee rate for sports markets
FEE_RATE_CRYPTO = 0.02  # Fee rate for crypto/finance/politics/tech markets
FEE_RATE_ECONOMICS = 0.015  # Fee rate for economics/culture/weather/other markets
MAKER_REBATE_PCT = 0.75  # Polymarket maker rebate rate — makers get 75% of fee back, paying ~25% effectively
MINIMUM_FEE = 0.0001  # Minimum fee amount in USDC

# ── Display ───────────────────────────────────────────────────────────────────

SUMMARY_DIV_WIDTH = 64  # Width of divider in summary output

# ── Validation ─────────────────────────────────────────────────────────────────

MAX_QUERY_LENGTH = 200  # Maximum query length for search
MAX_SEARCH_LIMIT = 100  # Maximum limit for search results
MIN_SEARCH_LIMIT = 1  # Minimum limit for search results
DEFAULT_SEARCH_LIMIT = 10  # Default limit for search results

# ── Precision ─────────────────────────────────────────────────────────────────

PRICE_ROUNDING = 6  # Decimal places for price rounding
FEE_ROUNDING = 6  # Decimal places for fee rounding
POLYMARKET_FEE_ROUNDING = 4  # Decimal places for Polymarket fee rounding


def calculate_polymarket_fee(shares: float, price: float, fee_rate: float) -> float:
    """Shared Polymarket fee formula: fee = shares * price * fee_rate * (price * (1 - price))^1."""
    exponent = 1
    fee = shares * price * fee_rate * (price * (1 - price)) ** exponent
    fee = round(fee, POLYMARKET_FEE_ROUNDING)
    return 0.0 if fee < MINIMUM_FEE else fee


def fee_rate_for_category(category: str) -> float:
    """Get the Polymarket fee rate for a given market category."""
    c = category.lower()
    if c == "sports":
        return FEE_RATE_SPORTS
    elif c in ("crypto", "finance", "politics", "tech"):
        return FEE_RATE_CRYPTO
    elif c in ("economics", "culture", "weather", "other"):
        return FEE_RATE_ECONOMICS
    return FEE_RATE_CRYPTO
SHARE_ROUNDING = 6  # Decimal places for share rounding
DISPLAY_ROUNDING_SHARES = 4  # Decimal places for displaying shares
DISPLAY_ROUNDING_PRICES = 4  # Decimal places for displaying prices
DISPLAY_ROUNDING_PNL = 4  # Decimal places for displaying P&L
DISPLAY_ROUNDING_PNL_PCT = 2  # Decimal places for displaying P&L percentage

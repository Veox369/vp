import json
import time
import logging
import telebot
import datetime
import requests
import re
import os
import sys
import psutil
import signal
import subprocess
from google import genai
from bs4 import BeautifulSoup
from collections import defaultdict
import time

# =============== Supervisor System ===============

class BotSupervisor:
    def __init__(self):
        self.max_consecutive_failures = 10
        self.min_recovery_delay = 10
        self.max_recovery_delay = 300  # 5 minutes
        self.process = None
        self.consecutive_failures = 0
        self.last_restart_time = 0
        
    def get_recovery_delay(self):
        """Calculate recovery delay with exponential backoff."""
        delay = min(self.max_recovery_delay, 
                   self.min_recovery_delay * (2 ** self.consecutive_failures))
        return delay
        
    def start_bot(self):
        """Start the bot process."""
        try:
            # Start bot as a separate Python process
            cmd = [sys.executable, __file__, "--supervised"]
            self.process = subprocess.Popen(cmd)
            logger.info("Bot process started with PID: %d", self.process.pid)
            self.last_restart_time = time.time()
            return True
        except Exception as e:
            logger.error("Failed to start bot process: %s", str(e))
            return False
            
    def check_memory_usage(self):
        """Check if memory usage is within acceptable limits."""
        if self.process:
            try:
                process = psutil.Process(self.process.pid)
                memory_percent = process.memory_percent()
                if memory_percent > 80:  # Restart if using >80% memory
                    logger.warning("High memory usage (%f%%), restarting bot", 
                                 memory_percent)
                    return False
            except Exception as e:
                logger.error("Error checking memory usage: %s", str(e))
        return True
        
    def supervise(self):
        """Main supervision loop."""
        logger.info("Starting bot supervisor...")
        
        while True:
            try:
                # Start bot if not running
                if not self.process or self.process.poll() is not None:
                    if self.start_bot():
                        self.consecutive_failures = 0
                    else:
                        self.consecutive_failures += 1
                
                # Check memory usage
                if not self.check_memory_usage():
                    self.process.terminate()
                    self.process.wait(timeout=30)
                    continue
                
                # Check if bot has been running for too long (restart every 12 hours)
                if time.time() - self.last_restart_time > 43200:  # 12 hours
                    logger.info("Performing scheduled restart")
                    self.process.terminate()
                    self.process.wait(timeout=30)
                    continue
                
                # Check if process has failed too many times
                if self.consecutive_failures >= self.max_consecutive_failures:
                    logger.critical(
                        "Too many consecutive failures (%d). Supervisor stopping.", 
                        self.consecutive_failures
                    )
                    break
                
                # Sleep before next check
                time.sleep(30)  # Check every 30 seconds
                
            except KeyboardInterrupt:
                logger.info("Supervisor stopped by user")
                if self.process:
                    self.process.terminate()
                    self.process.wait(timeout=30)
                break
            except Exception as e:
                logger.error("Supervisor error: %s", str(e))
                self.consecutive_failures += 1
                time.sleep(self.get_recovery_delay())

# Function to check if running as supervised process
def is_supervised():
    return "--supervised" in sys.argv


# =============== Rate Limit Handler ===============
class RateLimitHandler:
    def __init__(self):
        self.requests_timestamps = defaultdict(list)  # Track timestamps per key
        self.daily_counts = defaultdict(int)  # Track daily requests per key
        self.last_reset = {}  # Track when daily counts were last reset
        
    def can_use_key(self, key):
        current_time = time.time()
        
        # Clean old timestamps
        self.requests_timestamps[key] = [
            ts for ts in self.requests_timestamps[key] 
            if current_time - ts < 60  # Keep last minute only
        ]
        
        # Check RPM (30 requests per minute)
        if len(self.requests_timestamps[key]) >= 30:
            return False
            
        # Check RPD and reset if needed (1,500 requests per day)
        if current_time - self.last_reset.get(key, 0) >= 86400:  # 24 hours
            self.daily_counts[key] = 0
            self.last_reset[key] = current_time
            
        if self.daily_counts[key] >= 1500:
            return False
            
        return True
        
    def log_request(self, key):
        current_time = time.time()
        self.requests_timestamps[key].append(current_time)
        self.daily_counts[key] += 1
        
    def get_next_available_key(self, keys):
        """Find the next available key that hasn't hit rate limits."""
        for i in range(len(keys)):
            key = keys[i]
            if self.can_use_key(key):
                return i
        return None
        
    def wait_for_reset(self):
        """Sleep until the next minute to allow rate limits to reset."""
        time.sleep(61)  # Wait just over a minute

# =============== Configuration ===============

# Target channels to monitor
TARGET_CHANNELS = [ 
    'https://t.me/s/leaks_vip_signals',
    'https://t.me/s/Alex_100_x',
    'https://t.me/s/klondike_freemium',
    'https://t.me/s/Master_of_Bybit',
    'https://t.me/s/BinanceKillersVipChannel',
    'https://t.me/s/Always_winn',
    'https://t.me/s/Always_Win_Premium',
    'https://t.me/s/doublegtrading',
    'https://t.me/s/uscrypto2',
    'https://t.me/s/Binance_360_spot',
    'https://t.me/s/GG_Shot_Leak',
    'https://t.me/s/Binance_Pro_orginal',
    'https://t.me/s/BinancePumpSignaIs',
    'https://t.me/s/Master_of_binanace',
    'https://t.me/s/bananaleaks',
    'https://t.me/s/Crypto_leakk',
    'https://t.me/s/scalping_300_vip',
    'https://t.me/s/thebitcoindine',
    'https://t.me/s/CryptoKlondikePremium',
    'https://t.me/s/thebitcoindiner',
    'https://t.me/s/AlwaysWinChannel',
    'https://t.me/s/USABitcoinArmy',
    'https://t.me/s/AltSignal_VIP',
    'https://t.me/s/BinanceKillersVipOfficial',
    'https://t.me/s/FedRussianInsidersOfficial',
    'https://t.me/s/DLXTRADE',
    'https://t.me/s/THEWOLF_REAL',
    'https://t.me/s/crypto_musk1',
    'https://t.me/s/CryptoHeaven',
    'https://t.me/s/CryptoMonk_Japan',
    'https://t.me/s/k4kryptoo',
    'https://t.me/s/KingCryptoCalls',
    'https://t.me/s/arscryptocalls7',
    'https://t.me/s/Crypto_Freemium2',
    'https://t.me/s/cryptos_musk',
    'https://t.me/s/CryptoSharksTelegram',
    'https://t.me/s/black_whales_signals',
    'https://t.me/s/AlwaysWinChannel',
    'https://t.me/s/CryptoKlondikePremium',
    'https://t.me/s/thebitcoindiner',
    'https://t.me/s/Golden_Bull_Signals',
    'https://t.me/s/vipbaz_signal',
    'https://t.me/s/amanvipcrypto',
    'https://t.me/s/Market_Owners_Team',
    'https://t.me/s/doublegtrading',
    'https://t.me/s/Crypto_Safe_Calls',
    'https://t.me/s/binance_360',
    'https://t.me/s/gorilla_crypto',
    'https://t.me/s/crypto_freemium',
    'https://t.me/s/AltSignal_VIP',
    'https://t.me/s/crypto_freemium',
    'https://t.me/s/CryptoCoinsCoachCF',
    'https://t.me/s/signnal_free',
    'https://t.me/s/IRAN_LEAK',
    'https://t.me/s/best_signaaal',
    'https://t.me/s/bitunix_free',
    'https://t.me/s/doctor_traader',
    'https://t.me/s/whale_trade20',
    'https://t.me/s/uscrypto2',
    'https://t.me/s/CryptoLeaksd',
    'https://t.me/s/cryptoleaksz_group',
    'https://t.me/s/cryptoleakss',
    'https://t.me/s/Drvkich_Leaks',
    'https://t.me/s/RayanFuture',
    'https://t.me/s/Mahyar_Trade',
    'https://t.me/s/darabi_finance',
    'https://t.me/s/bitclub111',
    'https://t.me/s/Predictum_PRO',
    'https://t.me/s/predictumproleak',
    'https://t.me/s/GGShotLeakss',
    'https://t.me/s/predictumproo',
    'https://t.me/s/GG_Shot',
    'https://t.me/s/GG_shot_VVIP',
    'https://t.me/s/Free_GGSHOT'

]

# Track last processed message for each channel
last_processed_msgs = defaultdict(str)

# Cache for previously sent signals to prevent duplicates - Expanded from 15 to 100
MAX_CACHE_SIZE = 15

# Signal cache data structure - now using a list of dictionaries with timestamps
sent_signals_cache = []

# Persistent cache file
CACHE_FILE = "signal_cache.json"

# Keywords to look for in messages
KEYWORDS = ['long', 'short', 'sell', 'buy', "لانگ", "شورت"]

# Bot configuration
TELEGRAM_TOKEN = "8183212777:AAFzQZYcCLNZFYPeUquCfecO6ADrhmUfb1w"  # Get from @BotFather
TELEGRAM_CHANNEL_ID = "-1002658546073"  # Channel to forward signals to
TELEGRAM_CHANNEL_USERNAME = "@VeoxTrade"  # Channel username
GEMINI_KEYS = [    # Add more keys as needed
    "AIzaSyBCNB4lKToycJ7o80pjdyezROASl6ewhck",
    "AIzaSyBWgcETIilK1qjuCreKA3m65zI5byOVYJM",
    "AIzaSyCyuF9GiSLCJKTq6rWs0suadkJUREKJByA",
    "AIzaSyDCZ2K0YJ_DqmRUD0pkqitqY-aqEfUo0EA",
    "AIzaSyAhGQY7gOAA4vZ-L5PukVSQa5R5sXWinUU",
    "AIzaSyDQ8ffQ4xfkewCEhcb5UYNwsWPKFTBog04",
]
GEMINI_MODEL = 'gemini-2.0-flash'

# Initialize rate limit handler
rate_limit_handler = RateLimitHandler()

# Key rotation state
current_key_index = 0

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_next_gemini_client():
    """Get the next available Gemini client using smart key rotation."""
    global current_key_index, rate_limit_handler
    
    # Try to find an available key
    attempts = 0
    while attempts < len(GEMINI_KEYS) * 2:  # Try each key twice before giving up
        # Get next available key index
        available_index = rate_limit_handler.get_next_available_key(GEMINI_KEYS)
        
        if available_index is not None:
            current_key = GEMINI_KEYS[available_index]
            current_key_index = available_index
            
            # Get abbreviated key for logging
            key_preview = current_key[:8] + "..." if len(current_key) > 8 else current_key
            logger.info(f"Using Gemini API Key {current_key_index + 1} ({key_preview})")
            
            # Log this request for rate limiting
            rate_limit_handler.log_request(current_key)
            
            return genai.Client(api_key=current_key)
            
        # No available keys, wait for rate limits to reset
        logger.warning("All API keys are rate limited. Waiting for reset...")
        rate_limit_handler.wait_for_reset()
        attempts += 1
        
    raise Exception("All API keys are exhausted. Please check key validity and rate limits.")

# Initialize the bot and record start time
bot = telebot.TeleBot(TELEGRAM_TOKEN)
start_time = datetime.datetime.now()

# =============== Cache Persistence Functions ===============

def load_cache():
    """Load the signal cache from disk if it exists."""
    global sent_signals_cache
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, 'r') as f:
                cache_data = json.load(f)
                # Convert the loaded data into our cache format
                sent_signals_cache = cache_data
                logger.info(f"Loaded {len(sent_signals_cache)} signals from cache file")
    except Exception as e:
        logger.error(f"Error loading cache file: {str(e)}")
        # Start with empty cache if there's a problem
        sent_signals_cache = []

def save_cache():
    """Save the current signal cache to disk."""
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump(sent_signals_cache, f)
        logger.info(f"Saved {len(sent_signals_cache)} signals to cache file")
    except Exception as e:
        logger.error(f"Error saving cache file: {str(e)}")

# =============== Message Template ===============

MESSAGE_TEMPLATE = """
🪙  • {pair}

•—•—•—•—•—•—•

{position_emoji}  •  {position_type}

•—•—•—•—•—•—•

🎚  •  LEVERAGE : ×{leverage}

•—•—•—•—•—•—•

🎟  •  ENTRY : {entry_price}

•—•—•—•—•—•—•

☄️  •  TP :
{take_profits}

•—•—•—•—•—•—•

❗️  •  STOPLOSS :
{stop_loss}

•—•—•—•—•—•—•

{source_section}💀🔥 • @VeoxTrade
"""

SOURCE_TEMPLATE = """💫 FROM: {source_link}

•—•—•—•—•—•—•

"""

# =============== Gemini Prompts ===============

# Gemini prompt
GEMINI_PROMPT = """You are a trading signal parser. Your task is to extract trading information from messages and format it as JSON.

When I give you a message, respond ONLY with a JSON object containing these exact fields:
{
    "pair": "string",         // The trading pair in caps and alwaes use "/" befor usdt and usd (e.g., "BTC/USDT")
    "position_type": "string", // Must be exactly "LONG" or "SHORT"
    "leverage": "number",      // The leverage value (e.g., 50)
    "entry_price": "string",   // The entry price or "MARKET" if not specified
    "take_profits": ["string"], // Array of take profit prices
    "stop_loss": ["string"]    // Array of stop loss prices
}

Rules:
1. ONLY output valid JSON, nothing else
2. Don't include explanations or additional text
3. Use null for any missing fields
4. Always use the exact field names shown above
5. For position_type, only use "LONG" or "SHORT" (uppercase)
6. Convert all numbers to strings in take_profits and stop_loss arrays

Example input:
"LONG BTC/USDT with 50x leverage
Entry at 65000
TP: 66000, 67000, 68000
SL: 64000"

Example output:
{
    "pair": "BTC/USDT",
    "position_type": "LONG",
    "leverage": 50,
    "entry_price": "65000",
    "take_profits": ["66000", "67000", "68000"],
    "stop_loss": ["64000"]
}
"""

# New validation prompt
VALIDATION_PROMPT = """You are a sophisticated trading signal validator specialized in cryptocurrency markets. Your task is to determine if a message contains a valid cryptocurrency trading signal by carefully analyzing its content.

## Response Format

When analyzing a message, respond ONLY with a JSON object using this structure:
```json
{
    "is_valid_signal": true/false,
    "reason": "brief explanation",
    "details": {
        "trading_pair": "detected pair or null",
        "position": "detected position (LONG/SHORT/BUY/SELL) or null",
        "entry_price": "detected entry price or null",
        "take_profit": ["array of take profit levels or empty"],
        "stop_loss": "detected stop loss or null",
        "leverage": "detected leverage or null",
        "timeframe": "detected timeframe or null",
        "confidence": 0-100 (percentage of confidence in signal validity)
    }
}
Signal Validation Rules
A valid trading signal MUST contain AT LEAST:

A trading pair, which could be represented in various formats:

Standard format with slash: BTC/USDT, ETH/USD, SOL/USDT
With hyphen or underscore: BTC-USDT, ETH_USD
Without separator: BTCUSDT, ETHUSDC
Common abbreviations: Bitcoin, Ethereum, etc. paired with a fiat/stablecoin


A position direction, which could be expressed as:

Explicit terms: LONG, SHORT, BUY, SELL
Implicit indicators: "going up" (LONG), "bearish" (SHORT)
Emoji indicators: 🚀 or ⬆️ (LONG/BUY), 🔻 or ⬇️ (SHORT/SELL)
Context clues: "resistance break" (LONG), "support broken" (SHORT)


At least one of the following:

Entry price/zone (specific price or "entry now")
Take profit target(s)
Stop loss level



Signal Detection Techniques
Look for these components to identify valid signals:

Trading Pairs:

Check for cryptocurrency names (Bitcoin, BTC, Ethereum, ETH, etc.)
Identify standard trading pairs with any stablecoin or fiat (USDT, USDC, USD, EUR, etc.)
Recognize non-standard pairs (BTC/ETH, SOL/AVAX, etc.)


Position Direction:

Direct terms: LONG, SHORT, BUY, SELL
Context-based direction: "going to pump" = LONG, "dump incoming" = SHORT
Technical terms: "breakout" often implies LONG, "breakdown" often implies SHORT
Emoji analysis: Bull emojis (🐂, 🚀) suggest LONG, bear emojis (🐻, 📉) suggest SHORT


Price Information:

Entry points: Look for terms like "entry," "enter at," "entry zone," "current price"
Take profits: "TP," "target," "take profit," "exit at," multiple levels often numbered
Stop loss: "SL," "stop," "cut loss at," "exit if below/above"


Additional Signal Components (not required but strengthen validity):

Leverage: "10x," "leverage: 5x," etc.
Timeframe: "4H chart," "daily," "scalp," "swing trade," etc.
Risk/reward ratio: "R = 3:1," etc.
Chart patterns or indicators: "RSI oversold," "double bottom," etc.



Examples of Valid Signals

"LONG BTC/USDT with 50x leverage Entry at 65000 TP: 66000, 67000 SL: 64000"
"Short ETH/USDT at 3400, target 3300, stop at 3500"
"BTC entry now 🔥 targets at 68K and 70K"
"Ethereum looking good for a long position - enter at 3200, targets 3350 and 3500"
"AVAX/USDT: Going short here at 35.6 with tight stop above 36.2, targets: 34, 32.5, 30"
"Bitcoin 🚀 Entry zone: 63800-64200, TP1: 65K, TP2: 67K, SL: 62.5K"
"$SOL ready to pump! Buy spot or 5x long at current price, exit at 145+"

Examples of Invalid Signals

"Market is looking bearish today"
"Who's ready for some trading today?"
"Check out our new platform at trading.example.com"
"BTC chart analysis coming soon"
"Is ETH a good investment?"
"Join our premium group for the best signals!"

Processing Instructions

First, scan for trading pairs by looking for cryptocurrency names or symbols.
Then look for position direction indicators (explicit or implicit).
Search for price information (entry, take profit, stop loss).
Analyze context if direction is not explicit.
Extract additional signal components to complete the details object.
Assign a confidence score based on how many components are present and how clearly they're stated.
Make your final determination on validity.

ONLY respond with the JSON object, nothing else.
"""

# =============== Helper Functions ===============

def retry_on_error(func, max_retries=6, delay=3):
    """Retry a function on error with exponential backoff."""
    def wrapper(*args, **kwargs):
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                wait_time = delay * (2 ** attempt)
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
    return wrapper

def format_take_profits(take_profits):
    """Format take profit levels as numbered list."""
    if not take_profits:
        return "1) WILL BE ANNOUNCED"
    return "\n".join(f"{i+1}) {tp}" for i, tp in enumerate(take_profits))

def format_stop_loss(stop_loss):
    """Format stop loss levels."""
    if not stop_loss:
        return "1) 5% - 10% "
    return "\n".join(f"{i+1}) {sl}" for i, sl in enumerate(stop_loss))

def get_channel_username(url):
    """Convert channel URL to username format"""
    match = re.search(r'https://t\.me/s/([^/]+)', url)
    if match:
        return f"@{match.group(1)}"
    return url

def get_signal_fingerprint(pair, position_type, entry_price=None):
    """Creates a unique identifier for a signal based on its properties"""
    # Clean and standardize the values for comparison
    if pair:
        pair = pair.upper().strip()
    if position_type:
        position_type = position_type.upper().strip()
    if entry_price and entry_price != "MARKET":
        # Normalize price to handle slight variations in decimal places
        try:
            # Round to 2 decimal places for consistency
            entry_price = str(round(float(entry_price), 2))
        except:
            # If can't convert to float, use as is
            pass
    
    return f"{pair}_{position_type}_{entry_price}"

def is_duplicate_signal(formatted_signal, signal_data=None):
    """
    Enhanced duplicate detection that checks both exact matches and key signal properties
    
    Args:
        formatted_signal (str): The formatted signal text
        signal_data (dict, optional): Dictionary with extracted signal data
        
    Returns:
        bool: True if duplicate detected, False otherwise
    """
    current_time = int(time.time())
    
    # 1. Basic exact match check against all cached signals
    for cached_signal in sent_signals_cache:
        if cached_signal.get('formatted_signal') == formatted_signal:
            logger.info("Duplicate signal detected (exact text match)")
            return True
    
    # 2. Fingerprint-based check if we have signal data
    if signal_data and isinstance(signal_data, dict):
        pair = signal_data.get('pair')
        position_type = signal_data.get('position_type')
        entry_price = signal_data.get('entry_price')
        
        if pair and position_type:
            current_fingerprint = get_signal_fingerprint(pair, position_type, entry_price)
            
            # Check against all cached signals (with fingerprints)
            for cached_signal in sent_signals_cache:
                cached_fingerprint = cached_signal.get('fingerprint')
                if cached_fingerprint and cached_fingerprint == current_fingerprint:
                    # Check if it's recent (within 6 hours)
                    signal_time = cached_signal.get('timestamp', 0)
                    if current_time - signal_time < 21600:  # 6 hours in seconds
                        logger.info(f"Duplicate signal detected (matching fingerprint: {current_fingerprint})")
                        return True
    
            # Also check for same pair + position within short time
            for cached_signal in sent_signals_cache:
                cached_pair = cached_signal.get('pair')
                cached_position = cached_signal.get('position_type')
                
                if cached_pair and cached_position and cached_pair == pair and cached_position == position_type:
                    # Check if it's very recent (within 1 hour)
                    signal_time = cached_signal.get('timestamp', 0)
                    if current_time - signal_time < 3600:  # 1 hour in seconds
                        logger.info(f"Possible duplicate: {pair} {position_type} signal detected within last hour")
                        return True
    
    # No duplicates found
    return False

@retry_on_error
def send_sticker_to_channel():
    """Send the sticker to the channel after a signal."""
    try:
        with open('sticker.webp', 'rb') as sticker:
            bot.send_sticker(TELEGRAM_CHANNEL_ID, sticker)
        logger.info(f"Sticker sent to channel {TELEGRAM_CHANNEL_USERNAME}")
        return True
    except Exception as e:
        logger.error(f"Failed to send sticker to channel: {str(e)}")
        raise

@retry_on_error
def forward_to_channel(formatted_signal, signal_data=None):
    """Forward the formatted signal to the channel if it's not a duplicate."""
    try:
        # Check for duplicates first - enhanced version
        if is_duplicate_signal(formatted_signal, signal_data):
            logger.info("Signal not forwarded - duplicate detected")
            return False

        # Not a duplicate, proceed with sending
        msg = bot.send_message(TELEGRAM_CHANNEL_ID, formatted_signal)
        logger.info(f"Signal forwarded to channel {TELEGRAM_CHANNEL_USERNAME}")

        # Prepare cache entry with all relevant data
        current_time = int(time.time())
        cache_entry = {
            'formatted_signal': formatted_signal,
            'timestamp': current_time,
            'message_id': getattr(msg, 'message_id', None)
        }
        
        # Add signal data if available
        if signal_data and isinstance(signal_data, dict):
            cache_entry.update({
                'pair': signal_data.get('pair'),
                'position_type': signal_data.get('position_type'),
                'entry_price': signal_data.get('entry_price'),
                'fingerprint': get_signal_fingerprint(
                    signal_data.get('pair'), 
                    signal_data.get('position_type'),
                    signal_data.get('entry_price')
                )
            })
        
        # Add to our cache of sent signals
        sent_signals_cache.append(cache_entry)
        
        # Keep the cache at a reasonable size
        if len(sent_signals_cache) > MAX_CACHE_SIZE:
            sent_signals_cache.pop(0)  # Remove the oldest signal
            
        # Save cache to disk
        save_cache()

        return True
    except Exception as e:
        logger.error(f"Failed to forward signal to channel: {str(e)}")
        raise

@retry_on_error
def validate_signal(message_text):
    """Validate if the message is a proper trading signal using Gemini AI."""
    try:
        # Create prompt with user's message
        prompt = VALIDATION_PROMPT + "\n\nMessage:\n" + message_text

        # Get response from Gemini with key rotation
        client = get_next_gemini_client()
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt
        )

        # Parse JSON response from the text content
        try:
            # Get the response text and try to parse it as JSON
            response_text = response.text
            # Clean up the response text in case it contains markdown formatting
            if '```json' in response_text:
                response_text = response_text.split('```json')[1].split('```')[0].strip()
            elif '```' in response_text:
                response_text = response_text.split('```')[1].split('```')[0].strip()

            validation_result = json.loads(response_text)

            # Log the validation result
            logger.info(f"Signal validation result: {validation_result}")

            return validation_result.get('is_valid_signal', False), validation_result.get('reason', 'Unknown reason')

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse validation JSON: {response_text}")
            return False, "Failed to validate signal format"

    except Exception as e:
        logger.error(f"Error validating message: {str(e)}")
        return False, f"Validation error: {str(e)}"

@retry_on_error
def process_message_with_gemini(message_text, source_url=None):
    """Process message text using Gemini AI to extract trading information."""
    try:
        # First validate if this is a proper trading signal
        is_valid, reason = validate_signal(message_text)
        if not is_valid:
            logger.info(f"Message rejected as invalid signal: {reason}")
            return None, reason, None

        # Create prompt with user's message
        prompt = GEMINI_PROMPT + "\n\nMessage:\n" + message_text

        # Get response from Gemini with key rotation
        client = get_next_gemini_client()
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt
        )


        # Parse JSON response from the text content
        try:
            # Get the response text and try to parse it as JSON
            response_text = response.text
            # Clean up the response text in case it contains markdown formatting
            if '```json' in response_text:
                response_text = response_text.split('```json')[1].split('```')[0].strip()
            elif '```' in response_text:
                response_text = response_text.split('```')[1].split('```')[0].strip()

            data = json.loads(response_text)

            # Handle default leverage value
            if data.get('leverage') is None:
                data['leverage'] = 36

            # Validate required fields
            if not data.get('pair') or not data.get('position_type'):
                return None, "Missing required trading information", None

            # Check for duplicates using the extracted data
            if is_duplicate_signal(None, data):
                logger.info("Signal rejected as duplicate based on content")
                return None, "This appears to be a duplicate of a recent signal", None

            # Format source section if URL is provided
            source_section = ""
            if source_url:
                username = get_channel_username(source_url)
                if username:
                    source_section = SOURCE_TEMPLATE.format(source_link=username)

            # Format the message
            formatted_message = MESSAGE_TEMPLATE.format(
                pair=data['pair'],
                position_emoji='🟢' if data['position_type'] == 'LONG' else '🔴',
                position_type=data['position_type'],
                leverage=data.get('leverage', 'Not specified'),
                entry_price=data.get('entry_price', 'MARKET'),
                take_profits=format_take_profits(data.get('take_profits', [])),
                stop_loss=format_stop_loss(data.get('stop_loss', [])),
                source_section=source_section
            )

            # Check for unwanted pattern in formatted message
            if "• LEVERAGE : ×36" in formatted_message and \
               "• ENTRY : None" in formatted_message and \
                "• ENTRY : MARKET" in formatted_message and \
               "WILL BE ANNOUNCED" in formatted_message:
                logger.info("Message rejected: matches unwanted pattern (x36, no entry, only TP announced)")
                return None, "Message contains unwanted pattern", None

            return formatted_message, None, data

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {response_text}")
            return None, "Failed to parse trading information from response", None

    except Exception as e:
        error_message = f"Error processing message: {str(e)}\n"
        error_message += "Please ensure your message contains the required trading information."
        logger.error(f"Error processing message: {str(e)}")
        return None, error_message, None

# =============== Bot Setup & Handlers ===============

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    """Handle /start and /help commands."""
    welcome_text = """
Welcome to the Trading Signal Formatter Bot! 🤖

I can help format your trading signals professionally. Simply send me your trading signal with:
- Trading pair (e.g., BTC/USDT)
- Position type (LONG or SHORT)
- Leverage (defaults to x36 if not specified)
- Entry price
- Take profit targets
- Stop loss

Commands:
/start - Show this help message
/status - Show bot status and API key info
/clearcache - Clear the signal cache (admin only)

I'll format it beautifully for you! 📊

Example message:
"LONG BTC/USDT with 50x leverage
Entry at 65000
TP: 66000, 67000, 68000
SL: 64000"
    """
    bot.reply_to(message, welcome_text)

@bot.message_handler(commands=['status'])
def send_status(message):
    """Handle /status command to show current API key info."""
    status_text = f"""
Bot Status:
• Total API Keys: {len(GEMINI_KEYS)}
• Current Key: {current_key_index + 1}
• Model: {GEMINI_MODEL}
• Channel: {TELEGRAM_CHANNEL_USERNAME}
• Running since: {start_time.strftime('%Y-%m-%d %H:%M:%S')}
• Signal Cache Size: {len(sent_signals_cache)}/{MAX_CACHE_SIZE}
    """
    bot.reply_to(message, status_text)

@bot.message_handler(commands=['clearcache'])
def clear_cache(message):
    """Handle /clearcache command to clear the signal cache."""
    # Admin-only command - replace with your user ID
    admin_ids = [123456789]  # Replace with actual admin IDs
    
    if message.from_user.id in admin_ids:
        # Clear the cache
        global sent_signals_cache
        sent_signals_cache = []
        save_cache()
        bot.reply_to(message, "✓ Signal cache cleared successfully!")
    else:
        bot.reply_to(message, "⚠️ Sorry, this command is for admins only.")

@bot.message_handler(func=lambda message: True)
def process_signal(message):
    """Process all other messages as trading signals."""
    try:
        # Inform user that processing is starting
        processing_msg = bot.reply_to(message, "Processing your signal... ⚙️")

        # Process the message - source URL is None for direct bot messages
        formatted_signal, error, signal_data = process_message_with_gemini(message.text, None)

        # Delete the processing message
        bot.delete_message(message.chat.id, processing_msg.message_id)

        if formatted_signal:
            # Send the formatted signal to the user
            bot.reply_to(message, formatted_signal)

            # Forward the signal and sticker to the channel
            try:
                # First check if it's a duplicate and send if not
                if forward_to_channel(formatted_signal, signal_data):
                    # Only send sticker if the signal was actually sent (not a duplicate)
                    try:
                        send_sticker_to_channel()
                    except Exception as e:
                        logger.error(f"Failed to send sticker to channel: {str(e)}")
                        # Don't raise sticker error to user
            except Exception as e:
                logger.error(f"Failed to forward signal to channel: {str(e)}")
                # Don't raise the error to the user, as the signal was still successfully processed
        else:
            # Message wasn't a valid signal
            bot.reply_to(message, f"⚠️ Invalid trading signal: {error}\n\nPlease ensure your message contains a trading pair and position type (LONG/SHORT).")

    except Exception as e:
        bot.reply_to(message, f"An error occurred: {str(e)}\nPlease try again.")

# =============== Channel Monitoring Function ===============

def check_channel(url):
    """Check only the latest message from a channel for keywords, ignoring replies."""
    try:
        # Fetch channel content
        response = requests.get(url)
        if response.status_code != 200:
            logger.error(f"Failed to fetch channel {url}: {response.status_code}")
            return

        # Parse HTML and get ONLY the last message
        soup = BeautifulSoup(response.text, 'html.parser')
        messages = soup.find_all('div', class_='tgme_widget_message')
        if not messages:  # No messages found
            return

        # Extract the most recent message only
        last_message = messages[-1]  # Get only the last message
        msg_id = last_message.get('data-post', '')

        # Skip if this latest message was already processed
        if not msg_id or msg_id == last_processed_msgs[url]:
            return

        # Check if it's a reply - skip if it is
        reply_check = last_message.find('a', class_='tgme_widget_message_reply')
        if reply_check:
            logger.debug(f"Skipping reply message in channel {url}")
            last_processed_msgs[url] = msg_id  # Mark reply as processed
            return

        # Get text from the latest message only
        text_elem = last_message.find('div', class_='tgme_widget_message_text')
        if not text_elem:
            last_processed_msgs[url] = msg_id  # Mark as processed
            return

        # Process only this single latest message
        text = text_elem.get_text()
        text_lower = text.lower()

        # Only process if it contains our keywords
        if any(keyword in text_lower for keyword in KEYWORDS):
            logger.info(f"Found matching keywords in channel {url}: {text[:50]}...")

            # Validate and process the signal
            # Process the message with the source URL
            formatted_signal, error, signal_data = process_message_with_gemini(text, url)

            if formatted_signal:
                logger.info(f"Valid signal detected. Forwarding to channel...")
                # forward_to_channel now handles duplicate checking internally
                if forward_to_channel(formatted_signal, signal_data):
                    try:
                        send_sticker_to_channel()
                    except Exception as e:
                        logger.error(f"Failed to send sticker: {str(e)}")
            else:
                logger.info(f"Invalid signal detected: {error}. Not forwarding to channel.")
        else:
            logger.debug(f"No keywords found in latest message from {url}")

        # Mark this latest message as processed regardless of keywords
        last_processed_msgs[url] = msg_id

    except Exception as e:
        logger.error(f"Error checking channel {url}: {str(e)}")

def channel_monitor():
    """Monitor channels for new messages with error recovery."""
    consecutive_errors = 0
    while True:
        try:
            for url in TARGET_CHANNELS:
                try:
                    check_channel(url)
                    consecutive_errors = 0  # Reset error counter on success
                except requests.exceptions.RequestException as e:
                    logger.warning(f"Network error checking {url}: {str(e)}")
                    time.sleep(5)  # Brief pause on network error
                    continue
                except Exception as e:
                    logger.error(f"Error checking {url}: {str(e)}")
                    continue
                    
            # Sleep between checks
            time.sleep(60)  # Check every 15 seconds
            
        except KeyboardInterrupt:
            logger.info("Channel monitor stopped by user")
            break
        except Exception as e:
            logger.error(f"Channel monitor error: {str(e)}")
            consecutive_errors += 1
            
            if consecutive_errors >= 5:
                logger.critical("Too many consecutive errors. Restarting channel monitor...")
                time.sleep(30)  # Wait before trying to recover
                consecutive_errors = 0
            else:
                time.sleep(5 * consecutive_errors)  # Exponential backoff

def cleanup_resources():
    """Cleanup system resources and temporary data."""
    try:
        # Clear any temporary files
        cache_dir = os.path.dirname(CACHE_FILE)
        if os.path.exists(cache_dir):
            for file in os.listdir(cache_dir):
                if file.endswith('.tmp'):
                    try:
                        os.remove(os.path.join(cache_dir, file))
                    except:
                        pass
        
        # Clear memory
        import gc
        gc.collect()
        
        # Reset connection pools
        import urllib3
        try:
            urllib3.disable_warnings()
            pool_manager = urllib3.PoolManager()
            pool_manager.clear()
        except:
            pass
            
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")

def run_bot_with_recovery():
    """Run the bot with automatic recovery on failures."""
    consecutive_failures = 0
    start_time = time.time()
    
    while True:
        try:
            # Cleanup resources periodically
            if time.time() - start_time > 3600:  # Every hour
                cleanup_resources()
                start_time = time.time()
            
            # Load the signal cache from disk
            load_cache()
            
            # Start channel monitoring in a separate thread
            import threading
            monitor_thread = threading.Thread(target=channel_monitor)
            monitor_thread.daemon = True
            monitor_thread.start()

            # Start the bot with a timeout
            logger.info("Bot started. Press Ctrl+C to stop.")
            bot.polling(none_stop=True, timeout=60)
            
        except KeyboardInterrupt:
            logger.info("Bot stopped by user")
            save_cache()
            cleanup_resources()
            break
            
        except Exception as e:
            logger.error(f"Bot error: {str(e)}")
            save_cache()
            cleanup_resources()  # Clean up on error
            
            consecutive_failures += 1
            if consecutive_failures >= 5 and is_supervised():
                # If running as supervised process, keep retrying
                logger.error("Too many consecutive failures, waiting for supervisor to restart")
                time.sleep(30)
                consecutive_failures = 0
            elif consecutive_failures >= 5:
                logger.critical("Too many consecutive failures. Please check bot configuration.")
                break
            
            # Wait before trying to recover (exponential backoff)
            wait_time = min(300, 30 * (2 ** consecutive_failures))  # Cap at 5 minutes
            logger.info(f"Attempting recovery in {wait_time} seconds...")
            time.sleep(wait_time)

if __name__ == "__main__":
    run_bot_with_recovery()

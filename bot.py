import json
import time
import logging
import telebot
import datetime
import requests
import re
import os
from google import genai
from bs4 import BeautifulSoup
from collections import defaultdict

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
]

# Track last processed message for each channel
last_processed_msgs = defaultdict(str)

# Cache for previously sent signals to prevent duplicates - Expanded from 15 to 100
MAX_CACHE_SIZE = 100

# Signal cache data structure - now using a list of dictionaries with timestamps
sent_signals_cache = []

# Persistent cache file
CACHE_FILE = "signal_cache.json"

# Keywords to look for in messages
KEYWORDS = ['long', 'short', 'sell', 'buy', "لانگ", "شورت"]

# Bot configuration
TELEGRAM_TOKEN = "7971625984:AAGHWuslvYvoebhWHTp6Xg67ReC3iGddhU0"  # Get from @BotFather
TELEGRAM_CHANNEL_ID = "-1002414029295"  # Channel to forward signals to
TELEGRAM_CHANNEL_USERNAME = "@RexTrade101"  # Channel username
GEMINI_KEYS = [    # Add more keys as needed
    "AIzaSyBCNB4lKToycJ7o80pjdyezROASl6ewhck",
    "AIzaSyBWgcETIilK1qjuCreKA3m65zI5byOVYJM",
    "AIzaSyCyuF9GiSLCJKTq6rWs0suadkJUREKJByA",
    "AIzaSyDCZ2K0YJ_DqmRUD0pkqitqY-aqEfUo0EA",
    "AIzaSyAhGQY7gOAA4vZ-L5PukVSQa5R5sXWinUU",
    "AIzaSyDQ8ffQ4xfkewCEhcb5UYNwsWPKFTBog04",
]
GEMINI_MODEL = 'gemini-2.0-flash-lite'

# Key rotation state
current_key_index = 0

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_next_gemini_client():
    """Get the next Gemini client using key rotation."""
    global current_key_index
    current_key = GEMINI_KEYS[current_key_index]
    # Get abbreviated key for logging (first 8 chars)
    key_preview = current_key[:8] + "..." if len(current_key) > 8 else current_key
    logger.info(f"Using Gemini API Key {current_key_index + 1} ({key_preview})")

    # Rotate to next key
    current_key_index = (current_key_index + 1) % len(GEMINI_KEYS)
    return genai.Client(api_key=current_key)

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

{source_section}💀🔥 • @RexTrade101
"""

SOURCE_TEMPLATE = """📢  •  SOURCE :
{source_link}

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
VALIDATION_PROMPT = """You are a trading signal validator. Your task is to determine if a message contains a valid cryptocurrency trading signal.

When I give you a message, respond ONLY with a JSON object with this structure:
{
    "is_valid_signal": true/false,
    "reason": "brief explanation"
}

A valid trading signal MUST contain AT LEAST:
1. A trading pair (like BTC/USDT, ETH/USD, etc.)
2. A position direction (LONG/SHORT/BUY/SELL)

Examples of valid signals:
- "LONG BTC/USDT with 50x leverage Entry at 65000 TP: 66000, 67000 SL: 64000"
- "Short ETH/USDT at 3400, target 3300, stop at 3500"
- "BTC entry now 🔥 targets at 68K and 70K"

Examples of invalid signals:
- "Market is looking bearish today"
- "Who's ready for some trading today?"
- "Check out our new platform at trading.example.com"

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
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {response_text}")
            return None, "Failed to parse trading information from response", None

        # Validate required fields
        if not data.get('pair') or not data.get('position_type'):
            return None, "Missing required trading information", None

        # Check for duplicates using the extracted data
        if is_duplicate_signal(None, data):
            logger.info("Signal rejected as duplicate based on content")
            return None, "This appears to be a duplicate of a recent signal", None

        # Format source section if URL is provided
        source_section = SOURCE_TEMPLATE.format(source_link=source_url) if source_url else ""

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

        return formatted_message, None, data

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
    """Monitor channels for new messages."""
    try:
        while True:
            for url in TARGET_CHANNELS:
                check_channel(url)
            # Sleep between checks
            time.sleep(15)  # Check every 15 seconds
    except KeyboardInterrupt:
        logger.info("Channel monitor stopped by user")
    except Exception as e:
        logger.error(f"Channel monitor error: {str(e)}")
        raise

# =============== Main Execution ===============
if __name__ == "__main__":
    try:
        # Load the signal cache from disk
        load_cache()
        
        # Start channel monitoring in a separate thread
        import threading
        monitor_thread = threading.Thread(target=channel_monitor)
        monitor_thread.daemon = True
        monitor_thread.start()

        # Start the bot
        logger.info("Bot started. Press Ctrl+C to stop.")
        bot.polling(none_stop=True)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
        # Save cache on exit
        save_cache()
    except Exception as e:
        logger.error(f"Bot error: {str(e)}")
        # Save cache on error
        save_cache()
        raise

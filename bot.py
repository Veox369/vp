import json
import time
import logging
import telebot
import datetime
import requests
import re
from google import genai
from bs4 import BeautifulSoup
from collections import defaultdict

# =============== Configuration ===============

# Target channels to monitor
TARGET_CHANNELS = [
    "https://t.me/s/leaks_vip_signals",
    "https://t.me/s/Alex_100_x",
    "https://t.me/s/klondike_freemium",
    "https://t.me/s/Master_of_Bybit",
    "https://t.me/s/BinanceKillersVipChannel",
    "https://t.me/s/Always_winn",
    "https://t.me/s/Always_Win_Premium",
    "https://t.me/s/doublegtrading",
    "https://t.me/s/uscrypto2",
    "https://t.me/s/Binance_360_spot",
    "https://t.me/s/GG_Shot_Leak",
    "https://t.me/s/Binance_Pro_orginal",
    "https://t.me/s/BinancePumpSignaIs",
    "https://t.me/s/Master_of_binanace",
    "https://t.me/s/bananaleaks",
    "https://t.me/s/Crypto_leakk",
    "https://t.me/s/scalping_300_vip",
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
    'https://t.me/s/CryptoCoinsCoachCF'
]

# Track last processed message for each channel
last_processed_msgs = defaultdict(str)

# Keywords to look for in messages
KEYWORDS = ['long', 'short', 'sell', 'buy']

# Bot configuration
TELEGRAM_TOKEN = "7971625984:AAH-TSsdzkz4qFkiSeSwHUUNY7bGQ_oJbcY"  # Get from @BotFather
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
    """
    bot.reply_to(message, status_text)

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

# Message template
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

💀🔥 • @RexTrade101
"""

# Gemini prompt
GEMINI_PROMPT = """You are a trading signal parser. Your task is to extract trading information from messages and format it as JSON.

When I give you a message, respond ONLY with a JSON object containing these exact fields:
{
    "pair": "string",         // The trading pair in caps (e.g., "BTC/USDT")
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

def retry_on_error(func, max_retries=3, delay=2):
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
def forward_to_channel(formatted_signal):
    """Forward the formatted signal to the channel."""
    try:
        bot.send_message(TELEGRAM_CHANNEL_ID, formatted_signal)
        logger.info(f"Signal forwarded to channel {TELEGRAM_CHANNEL_USERNAME}")
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
def process_message_with_gemini(message_text):
    """Process message text using Gemini AI to extract trading information."""
    try:
        # First validate if this is a proper trading signal
        is_valid, reason = validate_signal(message_text)
        if not is_valid:
            logger.info(f"Message rejected as invalid signal: {reason}")
            return None, reason
            
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
            return None, "Failed to parse trading information from response"

        # Validate required fields
        if not data.get('pair') or not data.get('position_type'):
            return None, "Missing required trading information"

        # Format the message
        formatted_message = MESSAGE_TEMPLATE.format(
            pair=data['pair'],
            position_emoji='🟢' if data['position_type'] == 'LONG' else '🔴',
            position_type=data['position_type'],
            leverage=data.get('leverage', 'Not specified'),
            entry_price=data.get('entry_price', 'MARKET'),
            take_profits=format_take_profits(data.get('take_profits', [])),
            stop_loss=format_stop_loss(data.get('stop_loss', []))
        )

        return formatted_message, None

    except Exception as e:
        error_message = f"Error processing message: {str(e)}\n"
        error_message += "Please ensure your message contains the required trading information."
        logger.error(f"Error processing message: {str(e)}")
        return None, error_message

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

I'll format it beautifully for you! 📊

Example message:
"LONG BTC/USDT with 50x leverage
Entry at 65000
TP: 66000, 67000, 68000
SL: 64000"
    """
    bot.reply_to(message, welcome_text)

@bot.message_handler(func=lambda message: True)
def process_signal(message):
    """Process all other messages as trading signals."""
    try:
        # Inform user that processing is starting
        processing_msg = bot.reply_to(message, "Processing your signal... ⚙️")

        # Process the message using Gemini AI
        formatted_signal, error = process_message_with_gemini(message.text)

        # Delete the processing message
        bot.delete_message(message.chat.id, processing_msg.message_id)

        if formatted_signal:
            # Send the formatted signal to the user
            bot.reply_to(message, formatted_signal)

            # Forward the signal and sticker to the channel
            try:
                # First send the signal
                forward_to_channel(formatted_signal)

                # Then send the sticker
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

# =============== Main Execution ===============

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
            formatted_signal, error = process_message_with_gemini(text)
            
            if formatted_signal:
                logger.info(f"Valid signal detected. Forwarding to channel...")
                forward_to_channel(formatted_signal)
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
            time.sleep(30)  # Check every 30 seconds
    except KeyboardInterrupt:
        logger.info("Channel monitor stopped by user")
    except Exception as e:
        logger.error(f"Channel monitor error: {str(e)}")
        raise

if __name__ == "__main__":
    try:
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
    except Exception as e:
        logger.error(f"Bot error: {str(e)}")
        raise

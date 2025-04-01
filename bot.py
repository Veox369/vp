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
    "https://t.me/s/scalping_300_vip"
]

# Track last processed message for each channel
last_processed_msgs = defaultdict(str)

# Keywords to look for in messages
KEYWORDS = ['long', 'short']

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
GEMINI_MODEL = 'gemini-2.5-pro-exp-03-25'

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
def process_message_with_gemini(message_text):
    """Process message text using Gemini AI to extract trading information."""
    try:
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
            raise ValueError("Failed to parse trading information from response")

        # Validate required fields
        if not data.get('pair') or not data.get('position_type'):
            raise ValueError("Missing required trading information")

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

        return formatted_message

    except Exception as e:
        error_message = f"Error processing message: {str(e)}\n"
        error_message += "Please ensure your message contains the required trading information."
        logger.error(f"Error processing message: {str(e)}")
        return error_message

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
        formatted_signal = process_message_with_gemini(message.text)

        # Delete the processing message
        bot.delete_message(message.chat.id, processing_msg.message_id)

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
        text = text_elem.get_text().lower()
        
        # Only process if it contains our keywords
        if any(keyword in text for keyword in KEYWORDS):
            logger.info(f"Found matching keywords in channel {url}: {text[:50]}...")
            formatted_signal = process_message_with_gemini(text)
            if formatted_signal and "Error processing message" not in formatted_signal:
                forward_to_channel(formatted_signal)
                try:
                    send_sticker_to_channel()
                except Exception as e:
                    logger.error(f"Failed to send sticker: {str(e)}")
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
            time.sleep(30)  # Check every minute
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

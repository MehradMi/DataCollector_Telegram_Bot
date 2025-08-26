import logging
import os
import re
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from database import init_db, save_data_to_db
from time import gmtime, strftime
from module_openai_timestamp import calculate_timestamp

# Load environment variables
load_dotenv()
# ===========================

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
# ===========================

# --- Utility Function: Check whether text is a URL --- #
def is_url(text):
    """Check if text is a URL"""
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return url_pattern.match(text) is not None
# ===========================

# --- Utility Function: Parse date and category from text like 'date, category1/category2/..., description(optional)' --- #
def parse_date_categories_description(text):
    try:
        parts = text.strip().split(",")
        if len(parts) == 3:
            print(parts[0], parts[1], parts[2])
            return parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            return parts[0], parts[1], None
        return None, None, None
    except:
        return None, None, None
# ===========================


class DataCollector:
    def __init__(self):
        self.user_messages = {}
    
    def add_message(self, user_id, username, message):
        if user_id not in self.user_messages:
            self.user_messages[user_id] = {
                'messages': [],
                'username': username
            }

        self.user_messages[user_id]['username'] = username
        self.user_messages[user_id]['messages'].append(message)

        if len(self.user_messages[user_id]['messages']) >= 2:
            return self.process_messages(user_id)
        
        return None
    
    def process_messages(self, user_id):
        user_data = self.user_messages[user_id]
        messages = user_data['messages'][:2]
        username = user_data['username']

        url = None
        date = None
        categories = None
        description = None
        upload_status = "not uploaded"

        # Check which message is URL and which is date,categories, description(optional)
        for msg in messages:
            if is_url(msg):
                url = msg
            else:
                parsed_date, parsed_categories, parsed_description = parse_date_categories_description(msg)
                time_now = strftime("%Y-%m-%d %H:%M:%S", gmtime())
                if parsed_date and parsed_categories and parsed_description:
                    date = calculate_timestamp(parsed_date, time_now) # To return date in the YYYY, MM, DD HH:MM:SS format
                    categories = parsed_categories
                    description = parsed_description
                elif parsed_date and parsed_categories:
                    date = calculate_timestamp(parsed_date, time_now) # To return date in the YYYY, MM, DD HH:MM:SS format
                    categories = parsed_categories
                    description = "No description has been provided"

        # Remove processed messages
        self.user_messages[user_id]['messages'] = self.user_messages[user_id]['messages'][2:]

        # Validate we have all required data
        if url and date and categories and description:
            try:
                # Prepare data for storing in database
                categories_list = categories.strip().split("/")
                for category in categories_list:
                    data = {
                        'telegram_id': user_id,
                        'username': username,
                        'category': category,
                        'description': description,
                        'url': url,
                        'date': date,
                        'upload_status': upload_status
                    }
                
                    # Save to database
                    save_data_to_db(data)

                return {
                    "success": True,
                    "url": url,
                    "date": date,
                    "category": categories,
                    "description": description
                }
            except Exception as e:
                logger.error(f"Database error: {e}")
                return {
                    "success": False,
                    "error": "Failed to save data to database."
                }
        else:
            return {
                "success": False,
                "error": "Invalid format. Need one URL and one 'date, category1/category2/..., description(optional: can be omitted)' message."
            }

    def clear_user_messages(self, user_id):
        """Clear all pending messages for a user"""
        if user_id in self.user_messages:
            self.user_messages[user_id]["messages"] = []

# Creating a global data collector instance
data_collector = DataCollector()
# ===========================

# --- Bot Function: Start Command Handler --- #
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logger.info(f"User {user.id} ({user.username}) started the bot")
    
    await update.message.reply_text(
        f"Hello {user.first_name}! 👋\n\n"
        "Send me two messages:\n"
        "1️⃣ A URL\n"
        "2️⃣ Date and category in format: 'date,category1/category2/..., description(optional)'\n\n"
        "📅 Date formats supported:\n"
        "• 2h (2 hours ago)\n"
        "• 3d (3 days ago)\n"
        "• 1w (1 week ago)\n"
        "• 2m (2 months ago)\n"
        "• 1y (1 year ago)\n"
        "• 9 june (Current year's june 9th)\n\n"
        "Example: '2d,Technology/Computer Science' or '9 June, Sports/Football, An amazing goal'\n\n"
        "3️⃣ description: write your desrpiction as you want\n\n"
        "I'll process them automatically when I receive both.\n"
        "Use /reset to clear pending messages."
    )
# ===========================

# --- Bot Function: Incoming Messages Handler --- #
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user # Getting User_id (int)
    message_text = update.message.text

    logger.info(f"Received message from user {user.id} ({user.username}): {message_text}") 

    result = data_collector.add_message(user.id, user.username or user.first_name, message_text) # Adding message to collector

    if result is None:
        # Still awaiting the second message
        await update.message.reply_text("Got it! 📝 Send me the second message.")
    elif result["success"]:
        # Successfully processed
        await update.message.reply_text( 
            f"✅ Data saved successfully!\n\n"
            f"🔗 URL: {result['url']}\n"
            f"📅 Date: {result['date']}\n"
            f"📂 Categories: {result['category']}\n"
            f"📝 Description: {result['description']}"
        )
    else:
        error_msg = result.get("error", "Unknow error occurred")
        await update.message.reply_text(f"❌ Error: {error_msg}\n\nUse /reset to start over")
# ===========================

# --- Bot Function: Reset Command Handler --- #
async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    data_collector.clear_user_messages(user.id)
    await update.message.reply_text("🗑️ Your pending messages have been cleared. Send me two new messages!")
# ===========================

# --- Bot Function: Status Command Handler --- #
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    pending_count = 0

    if user.id in data_collector.user_messages:
        pending_count = len(data_collector.user_messages[user.id]['messages'])
    
    if pending_count == 0:
        await update.message.reply_text("📭 No pending messages. Send me a URL or 'date, category' message!")
    elif pending_count == 1:
        await update.message.reply_text("📬 You have 1 pending message. Send me one more!")
    else:
        await update.message.reply_text(f"📮 You have {pending_count} pending messages.")
# ===========================

# --- Main Function: Program Entry Point --- #
def main():
    """Main function to run the bot"""
    # Initialize database
    try:
        init_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return
    
    # Get bot token from environment
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not found in environment variables. Please check your .env file.")
        return

    # Create application
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("reset", reset))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Start the bot
    logger.info("Starting bot...")
    try:
        application.run_polling()
    except Exception as e:
        logger.error(f"Error running bot: {e}")
# ===========================

if __name__ == '__main__':
    main()

                    
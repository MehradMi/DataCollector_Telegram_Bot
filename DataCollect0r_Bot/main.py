import logging
import os
import re
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from database import init_db, save_data_to_db

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

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

def parse_date_category(text):
    """Parse date and category from text like 'date, category'"""
    try:
        parts = [part.strip() for part in text.split(',')]
        if len(parts) == 2:
            return parts[0], parts[1]
        return None, None
    except:
        return None, None

class DataCollector:
    def __init__(self):
        self.user_messages = {}  # Store messages per user
        
    def add_message(self, user_id, username, message):
        """Add a message for a user"""
        if user_id not in self.user_messages:
            self.user_messages[user_id] = {
                'messages': [],
                'username': username
            }
        
        # Update username in case it changed
        self.user_messages[user_id]['username'] = username
        self.user_messages[user_id]['messages'].append(message)
        
        # If we have 2 messages, process them
        if len(self.user_messages[user_id]['messages']) >= 2:
            return self.process_messages(user_id)
        
        return None
    
    def process_messages(self, user_id):
        """Process the two messages for a user"""
        user_data = self.user_messages[user_id]
        messages = user_data['messages'][:2]  # Take first 2 messages
        username = user_data['username']
        
        url = None
        date = None
        category = None
        
        # Check which message is URL and which is date,category
        for msg in messages:
            if is_url(msg):
                url = msg
            else:
                parsed_date, parsed_category = parse_date_category(msg)
                if parsed_date and parsed_category:
                    date = parsed_date
                    category = parsed_category
        
        # Remove processed messages
        self.user_messages[user_id]['messages'] = self.user_messages[user_id]['messages'][2:]
        
        # Validate we have all required data
        if url and date and category:
            try:
                # Prepare data for database
                data = {
                    'telegram_id': user_id,
                    'username': username,
                    'category': category,
                    'url': url,
                    'date': date
                }
                
                # Save to database
                save_data_to_db(data)
                
                return {
                    'success': True,
                    'url': url,
                    'date': date,
                    'category': category
                }
            except Exception as e:
                logger.error(f"Database error: {e}")
                return {
                    'success': False,
                    'error': 'Failed to save data to database.'
                }
        else:
            return {
                'success': False,
                'error': 'Invalid format. Need one URL and one "date, category" message.'
            }
    
    def clear_user_messages(self, user_id):
        """Clear all pending messages for a user"""
        if user_id in self.user_messages:
            self.user_messages[user_id]['messages'] = []

# Global data collector instance
data_collector = DataCollector()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    user = update.effective_user
    logger.info(f"User {user.id} ({user.username}) started the bot")
    
    await update.message.reply_text(
        f"Hello {user.first_name}! 👋\n\n"
        "Send me two messages:\n"
        "1️⃣ A URL\n"
        "2️⃣ Date and category in format: 'date, category'\n\n"
        "I'll process them automatically when I receive both.\n"
        "Use /reset to clear pending messages."
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle incoming messages"""
    user = update.effective_user
    message_text = update.message.text
    
    logger.info(f"Received message from user {user.id} ({user.username}): {message_text}")
    
    # Add message to collector
    result = data_collector.add_message(user.id, user.username or user.first_name, message_text)
    
    if result is None:
        # Still waiting for second message
        await update.message.reply_text("Got it! 📝 Send me the second message.")
    elif result['success']:
        # Successfully processed
        await update.message.reply_text(
            f"✅ Data saved successfully!\n\n"
            f"🔗 URL: {result['url']}\n"
            f"📅 Date: {result['date']}\n"
            f"📂 Category: {result['category']}"
        )
    else:
        # Error in processing
        error_msg = result.get('error', 'Unknown error occurred')
        await update.message.reply_text(f"❌ Error: {error_msg}\n\nUse /reset to start over.")

async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reset user's pending messages"""
    user = update.effective_user
    data_collector.clear_user_messages(user.id)
    await update.message.reply_text("🗑️ Your pending messages have been cleared. Send me two new messages!")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user's pending message count"""
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

if __name__ == '__main__':
    main()
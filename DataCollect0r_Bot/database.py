import os
import sqlite3
import logging

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE_PATH = os.path.join(BASE_DIR, "dataset.db")

def init_db():
    """Initialize the database and create tables if they don't exist"""
    try:
        conn = sqlite3.connect(DB_FILE_PATH, check_same_thread=False)
        cur = conn.cursor()
        cur.execute("""
CREATE TABLE IF NOT EXISTS dataset (
                 telegram_id INT,
                 username TEXT,
                 category TEXT,
                 url TEXT,
                 date TEXT,
                 description TEXT,
                 UNIQUE (telegram_id, url, category)
                 )
""")
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

def save_data_to_db(data):
    """Save data to the database with conflict resolution"""
    try:
        # Validate required fields
        required_fields = ['telegram_id', 'username', 'category', 'url', 'date']
        for field in required_fields:
            if field not in data or data[field] is None:
                raise ValueError(f"Missing required field: {field}")
        
        conn = sqlite3.connect(DB_FILE_PATH, check_same_thread=False)
        cur = conn.cursor()
        
        cur.execute("""
INSERT INTO dataset VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (telegram_id, url, category) DO UPDATE SET
                    username = excluded.username,
                    date = excluded.date,
                    description = excluded.description
""", (
        data.get("telegram_id"),
        data.get("username"),
        data.get("category"),
        data.get("url"),
        data.get("date"),
        data.get("description", "")
    )) 

        conn.commit()
        conn.close()
        
        logger.info(f"Data saved for user {data.get('telegram_id')}: {data.get('url')} - {data.get('category')}")
        
    except Exception as e:
        logger.error(f"Failed to save data to database: {e}")
        raise

def get_user_data(telegram_id):
    """Get all data for a specific user (optional utility function)"""
    try:
        conn = sqlite3.connect(DB_FILE_PATH, check_same_thread=False)
        cur = conn.cursor()
        
        cur.execute("SELECT * FROM dataset WHERE telegram_id = ?", (telegram_id,))
        results = cur.fetchall()
        
        conn.close()
        return results
        
    except Exception as e:
        logger.error(f"Failed to retrieve user data: {e}")
        return []

def get_all_data():
    """Get all data from database (optional utility function)"""
    try:
        conn = sqlite3.connect(DB_FILE_PATH, check_same_thread=False)
        cur = conn.cursor()
        
        cur.execute("SELECT * FROM dataset")
        results = cur.fetchall()
        
        conn.close()
        return results
        
    except Exception as e:
        logger.error(f"Failed to retrieve all data: {e}")
        return []
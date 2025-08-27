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
        
        # Create the "dataset" table
        cur.execute("""
CREATE TABLE IF NOT EXISTS dataset (
                 telegram_id INT,
                 username TEXT,
                 category TEXT,
                 url TEXT,
                 date TEXT,
                 description TEXT,
                 upload_status TEXT,
                 UNIQUE (telegram_id, url, category)
                 )
""")
        conn.commit()
        conn.close()
        
        conn = sqlite3.connect(DB_FILE_PATH, check_same_thread=False)
        cur = conn.cursor()

        # Create the "dataset_backup" table
        cur.execute("""
CREATE TABLE IF NOT EXISTS dataset_backup (
                 telegram_id INT,
                 username TEXT,
                 category TEXT,
                 url TEXT,
                 date TEXT,
                 description TEXT,
                 upload_status TEXT,
                 download_status TEXT DEFAULT 'not_downloaded'
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
INSERT INTO dataset VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (telegram_id, url, category) DO UPDATE SET
                    username = excluded.username,
                    date = excluded.date,
                    upload_status = excluded.upload_status,
                    description = excluded.description
""", (
        data.get("telegram_id"),
        data.get("username"),
        data.get("category"),
        data.get("url"),
        data.get("date"),
        data.get("description"),
        data.get("upload_status")
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
    
def get_payload_data():
    try: 
        conn = sqlite3.connect(DB_FILE_PATH, check_same_thread=False)
        cur = conn.cursor()

        cur.execute("SELECT rowid, telegram_id, username, url, category, date, upload_status, description FROM dataset")
        rows = cur.fetchall()

        conn.close()
        return rows

    except Exception as e:
        logger.error(f"Failed to retrieve payload data: {e}")
        return []
    
def change_upload_status(rowid, telegram_id, username, url, category, date, description, upload_status):
    upload_status = "uploaded"
    try:
        conn = sqlite3.connect(DB_FILE_PATH, check_same_thread=False)
        cur = conn.cursor()

        cur.execute(f"UPDATE dataset SET upload_status = '{upload_status}' WHERE rowid = ?", (rowid,))
        conn.commit()

        cur.execute(
                        """
                            INSERT INTO dataset_backup (telegram_id, username, category, url, date, description, upload_status) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (telegram_id, username, category, url, date, description, upload_status)
                    )
        
        # Delete from original
        cur.execute("DELETE FROM dataset WHERE rowid = ?", (rowid,))

        conn.commit()
        conn.close()
    
    except Exception as e:
        logger.error(f"Failed to update 'upload_status' column: {e}")
        return []

def get_download_url_data():
    """Fetch distinct URLs from dataset_backup that are not downloaded yet"""
    try:
        conn = sqlite3.connect(DB_FILE_PATH, check_same_thread=False)
        cur = conn.cursor()
        
        # Query all URLs with status "not_downloaded"
        cur.execute("SELECT DISTINCT url, rowid FROM dataset_backup WHERE download_status = 'not_downloaded'")
        rows = cur.fetchall()
        
        conn.close()
        return rows  # list of (url, rowid)
    
    except Exception as e:
        logger.error(f"Error fetching download URL data: {e}")
        return []

def change_download_status(rowid, status="downloaded"):
    """Change the download_status of a specific row in dataset_backup"""
    try:
        conn = sqlite3.connect(DB_FILE_PATH, check_same_thread=False)
        cur = conn.cursor()
        
        cur.execute(
            "UPDATE dataset_backup SET download_status = ? WHERE rowid = ?",
            (status, rowid)
        )
        
        conn.commit()
        conn.close()
        logger.info(f"Row {rowid} updated to status '{status}'")
        
    except Exception as e:
        logger.error(f"Error updating download status for rowid {rowid}: {e}")
    
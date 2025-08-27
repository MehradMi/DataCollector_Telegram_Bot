import os
import sqlite3
import requests
import logging
import time
import json
from pathlib import Path
from urllib.parse import urlparse
from dotenv import load_dotenv
from apify_client import ApifyClient
from database import DB_FILE_PATH, get_download_url_data, change_download_status

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class InstagramDownloader:
    def __init__(self, apify_token, actor_id="9JaThuZFzYiFtPXpc"):
        self.client = ApifyClient(apify_token)
        self.actor_id = actor_id
        
        # Create downloads directory if it doesn't exist
        self.download_dir = Path("downloads")
        self.download_dir.mkdir(exist_ok=True)

    def process_instagram_video(self, url, rowid):
        """Process Instagram video via Apify - trigger download and get metadata"""
        try:
            logger.info(f"Starting Apify processing for: {url}")

            # Input for the Apify Actor
            run_input = {
                "urls": [{"url": url}],
                "quality": "best",
                "format": "mp4",
                "concurrency": 5
            }

            # Run the actor
            run = self.client.actor(self.actor_id).call(run_input=run_input)
            
            # Check if run was successful
            if run and run.get("status") == "SUCCEEDED":
                logger.info(f"Apify run succeeded for {url}")
                
                # Get run info and dataset info
                run_id = run.get("id")
                dataset_id = run.get("defaultDatasetId")
                
                # Try to get results from dataset
                results = []
                try:
                    for item in self.client.dataset(dataset_id).iterate_items():
                        results.append(item)
                except Exception as e:
                    logger.warning(f"Could not iterate dataset items: {e}")
                
                # Save run metadata for later use
                metadata = {
                    "run_id": run_id,
                    "dataset_id": dataset_id,
                    "url": url,
                    "rowid": rowid,
                    "results_count": len(results),
                    "has_direct_download": bool(results),
                    "run_status": run.get("status"),
                    "created_at": time.time()
                }
                
                # Save metadata to file for webhook or manual processing
                self.save_processing_metadata(rowid, metadata)
                
                # If we have direct download URLs, try to download
                if results:
                    video_data = results[0]
                    download_url = (video_data.get("downloadURL") or 
                                  video_data.get("downloadUrl") or 
                                  video_data.get("url"))
                    
                    if download_url:
                        logger.info(f"Direct download URL found: {download_url}")
                        filename = self.download_video_file(download_url, rowid, url)
                        if filename:
                            metadata["local_file"] = filename
                            self.save_processing_metadata(rowid, metadata)
                
                # Mark as processed (videos are available in Apify panel even without direct URLs)
                change_download_status(rowid, "processed")
                logger.info(f"Successfully processed {url} - check Apify panel for downloads")
                
                return metadata
            else:
                logger.error(f"Apify run failed for {url}")
                change_download_status(rowid, "failed")
                return None

        except Exception as e:
            logger.error(f"Failed to process {url}: {e}")
            change_download_status(rowid, "failed")
            return None

    def download_video_file(self, download_url, rowid, original_url):
        """Download video file from direct URL"""
        try:
            logger.info(f"Downloading video from {download_url}")
            response = requests.get(download_url, stream=True, timeout=300)
            response.raise_for_status()

            timestamp = int(time.time())
            filename = f"instagram_video_{timestamp}_{rowid}.mp4"
            filepath = self.download_dir / filename
            
            with open(filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            logger.info(f"Video downloaded: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"Failed to download video file: {e}")
            return None

    def save_processing_metadata(self, rowid, metadata):
        """Save processing metadata to database"""
        try:
            conn = sqlite3.connect(DB_FILE_PATH, check_same_thread=False)
            cur = conn.cursor()
            
            # Create metadata table if it doesn't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS processing_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    backup_rowid INTEGER,
                    metadata_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert or update metadata
            cur.execute("""
                INSERT OR REPLACE INTO processing_metadata (backup_rowid, metadata_json, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, (rowid, json.dumps(metadata)))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Metadata saved for rowid {rowid}")
            
        except Exception as e:
            logger.error(f"Failed to save metadata for rowid {rowid}: {e}")

    def get_apify_download_urls(self, rowid):
        """Get download URLs from Apify for a processed video"""
        try:
            conn = sqlite3.connect(DB_FILE_PATH, check_same_thread=False)
            cur = conn.cursor()
            
            cur.execute("SELECT metadata_json FROM processing_metadata WHERE backup_rowid = ?", (rowid,))
            result = cur.fetchone()
            conn.close()
            
            if result:
                metadata = json.loads(result[0])
                dataset_id = metadata.get("dataset_id")
                
                if dataset_id:
                    # Get fresh data from Apify
                    results = []
                    for item in self.client.dataset(dataset_id).iterate_items():
                        results.append(item)
                    
                    if results:
                        video_data = results[0]
                        download_url = (video_data.get("downloadURL") or 
                                      video_data.get("downloadUrl") or 
                                      video_data.get("url"))
                        return download_url
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get Apify download URLs for rowid {rowid}: {e}")
            return None


# Simple webhook receiver (optional - for Apify webhook notifications)
def create_webhook_handler():
    """Create a simple webhook handler file"""
    webhook_code = '''
import json
import logging
from flask import Flask, request, jsonify
from database import change_download_status

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/apify-webhook', methods=['POST'])
def handle_apify_webhook():
    """Handle Apify webhook notifications"""
    try:
        data = request.json
        logger.info(f"Received webhook: {data}")
        
        # Extract run information
        run_id = data.get("resource", {}).get("id")
        status = data.get("resource", {}).get("status")
        
        if status == "SUCCEEDED":
            # Update database based on run_id
            # You'll need to map run_id to rowid somehow
            logger.info(f"Run {run_id} completed successfully")
        
        return jsonify({"status": "ok"})
        
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
'''
    
    with open("webhook_handler.py", "w") as f:
        f.write(webhook_code)
    
    logger.info("Webhook handler created: webhook_handler.py")


# ------------------- Main -------------------

def main():
    apify_token = os.getenv("APIFY_API_TOKEN")
    if not apify_token:
        logger.error("Missing APIFY_API_TOKEN in environment variables")
        return

    downloader = InstagramDownloader(apify_token)

    # Get URLs to process
    urls = get_download_url_data()
    if not urls:
        logger.info("No videos pending download.")
        return

    logger.info(f"Processing {len(urls)} videos...")

    for url, rowid in urls:
        logger.info(f"Processing video {rowid}: {url}")
        
        metadata = downloader.process_instagram_video(url, rowid)
        
        if metadata:
            logger.info(f"Video {rowid} processed - Run ID: {metadata.get('run_id')}")
            if metadata.get('local_file'):
                logger.info(f"Local file: {metadata['local_file']}")
            else:
                logger.info("Video available in Apify panel")
        else:
            logger.warning(f"Failed to process video {rowid}")
        
        # Rate limiting
        time.sleep(8)

    logger.info("Processing complete. Check Apify panel for downloaded videos.")


if __name__ == "__main__":
    main()
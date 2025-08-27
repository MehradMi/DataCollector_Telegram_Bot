import os
import sqlite3
import requests
import logging
import time
import tempfile
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

    def download_instagram_video(self, url, rowid):
        """Download Instagram video via Apify and save locally"""
        try:
            logger.info(f"Starting Apify download for: {url}")

            # Input for the Apify Actor
            run_input = {
                "urls": [{"url": url}],
                "quality": "best",
                "format": "default",
            }

            run = self.client.actor(self.actor_id).call(run_input=run_input)

            # Get results
            results = []
            for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                results.append(item)

            if not results:
                logger.error(f"No results returned from Apify for {url}")
                change_download_status(rowid, "failed")
                return None

            video_data = results[0]
            video_url = video_data.get("downloadUrl") or video_data.get("url")

            if not video_url:
                logger.error(f"No download URL found in response for {url}")
                logger.info(f"Available fields: {list(video_data.keys())}")
                change_download_status(rowid, "failed")
                return None

            # Download the file
            logger.info(f"Downloading from {video_url}")
            response = requests.get(video_url, stream=True, timeout=300)
            response.raise_for_status()

            filename = f"instagram_video_{int(time.time())}.mp4"
            with open(filename, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            logger.info(f"Video saved as {filename}")
            change_download_status(rowid, "downloaded")
            return filename

        except Exception as e:
            logger.error(f"Failed to download {url}: {e}")
            change_download_status(rowid, "failed")
            return None


# ------------------- Main -------------------

def main():
    apify_token = os.getenv("APIFY_API_TOKEN")
    if not apify_token:
        logger.error("Missing APIFY_API_TOKEN in environment variables")
        return

    downloader = InstagramDownloader(apify_token)

    urls = get_download_url_data()
    if not urls:
        logger.info("No videos pending download.")
        return

    for url, rowid in urls:
        downloader.download_instagram_video(url, rowid)
        time.sleep(5)  # prevent API spam


if __name__ == "__main__":
    main()
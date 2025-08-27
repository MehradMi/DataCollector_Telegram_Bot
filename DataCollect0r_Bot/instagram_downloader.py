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
from database import DB_FILE_PATH

# Load environment variables
load_dotenv()

pixoform_aws_upload_url = os.getenv("AWS_UPLOAD_URL")
pixoform_aws_delete_url = os.getenv("AWS_DELETE_URL")

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class ApifyInstagramDownloader:
    def __init__(self):
        self.apify_api_token = os.getenv('APIFY_API_TOKEN')
        if not self.apify_api_token:
            raise ValueError("APIFY_API_TOKEN not found in environment variables")
        
        self.client = ApifyClient(self.apify_api_token)
        self.aws_upload_url = pixoform_aws_upload_url
        self.aws_delete_url = pixoform_aws_delete_url
        self.temp_dir = tempfile.gettempdir()
        
        # Try different free Apify Instagram actors in order of preference
        self.actor_ids = [
            "shu8hvrXbJbY3Eb9W",  # Instagram Scraper (free tier available)
            "dSCLg0C3YEZ83HzYX",  # Another Instagram downloader
            "apify/instagram-scraper"  # Official Apify Instagram scraper
        ]
        
    def get_distinct_urls(self):
        """Get distinct URLs from dataset_backup table"""
        try:
            conn = sqlite3.connect(DB_FILE_PATH, check_same_thread=False)
            cur = conn.cursor()
            
            cur.execute("SELECT DISTINCT url, rowid, telegram_id, username, category, date, description FROM dataset_backup")
            results = cur.fetchall()
            
            conn.close()
            logger.info(f"Found {len(results)} distinct URLs to process")
            return results
            
        except Exception as e:
            logger.error(f"Failed to retrieve URLs from database: {e}")
            return []
    
    def download_instagram_video_with_apify(self, url):
        """Download Instagram video using Apify API"""
        try:
            logger.info(f"Starting Apify download for: {url}")
            
            # Configure the actor input
            run_input = {
                "urls": [url],
                "results": 1,
                "proxy": {
                    "useApifyProxy": True,
                    "apifyProxyGroups": ["RESIDENTIAL"]
                }
            }
            
            # Run the actor
            run = self.client.actor(self.actor_id).call(run_input=run_input)
            
            # Get results
            results = []
            for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                results.append(item)
            
            if not results:
                logger.error(f"No results returned from Apify for URL: {url}")
                return None
            
            # Extract video download URL from the first result
            video_data = results[0]
            video_url = None
            
            # Different actors return data in different formats, try common field names
            possible_fields = ['videoUrl', 'video_url', 'downloadUrl', 'url', 'videoDownloadUrl']
            for field in possible_fields:
                if field in video_data and video_data[field]:
                    video_url = video_data[field]
                    break
            
            if not video_url:
                logger.error(f"No video download URL found in Apify response for: {url}")
                logger.info(f"Available fields: {list(video_data.keys())}")
                return None
            
            # Download the video file from the URL provided by Apify
            logger.info(f"Downloading video from Apify URL: {video_url}")
            
            response = requests.get(video_url, stream=True, timeout=300)
            response.raise_for_status()
            
            # Generate local filename
            filename = f"instagram_video_{int(time.time())}.mp4"
            local_file_path = os.path.join(self.temp_dir, filename)
            
            # Save video to local file
            with open(local_file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            logger.info(f"Successfully downloaded video to: {local_file_path}")
            return local_file_path
            
        except Exception as e:
            logger.error(f"Failed to download video with Apify from {url}: {e}")
            return None
    
    def upload_to_aws(self, file_path, original_url, metadata):
        """Upload file to AWS via API"""
        try:
            # Prepare filename with metadata
            filename = os.path.basename(file_path)
            name_without_ext = os.path.splitext(filename)[0]
            ext = os.path.splitext(filename)[1]
            
            # Create descriptive filename
            new_filename = f"{metadata['telegram_id']}_{metadata['category']}_{name_without_ext}{ext}"
            
            with open(file_path, 'rb') as file:
                files = {'file': (new_filename, file, 'video/mp4')}
                
                response = requests.post(self.aws_upload_url, files=files, timeout=300)
                
                if response.status_code == 200:
                    result = response.json()
                    aws_url = result.get('file_url') or result.get('url')
                    logger.info(f"Successfully uploaded to AWS: {aws_url}")
                    
                    # Update database with AWS URL
                    self.update_database_with_aws_url(metadata['rowid'], aws_url)
                    
                    return aws_url
                else:
                    logger.error(f"AWS upload failed for {original_url}: {response.status_code} - {response.text}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error uploading {file_path} to AWS: {e}")
            return None
    
    def update_database_with_aws_url(self, rowid, aws_url):
        """Update database record with AWS URL"""
        try:
            conn = sqlite3.connect(DB_FILE_PATH, check_same_thread=False)
            cur = conn.cursor()
            
            # Add aws_url column if it doesn't exist
            try:
                cur.execute("ALTER TABLE dataset_backup ADD COLUMN aws_url TEXT")
                conn.commit()
            except sqlite3.OperationalError:
                # Column already exists
                pass
            
            # Update the record with AWS URL
            cur.execute("UPDATE dataset_backup SET aws_url = ? WHERE rowid = ?", (aws_url, rowid))
            conn.commit()
            conn.close()
            
            logger.info(f"Updated database record {rowid} with AWS URL")
            
        except Exception as e:
            logger.error(f"Failed to update database with AWS URL: {e}")
    
    def cleanup_local_file(self, file_path):
        """Remove local file after upload"""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Cleaned up local file: {file_path}")
        except Exception as e:
            logger.error(f"Failed to cleanup file {file_path}: {e}")
    
    def get_aws_urls_from_db(self, telegram_id=None, category=None):
        """Get AWS URLs from database with optional filters"""
        try:
            conn = sqlite3.connect(DB_FILE_PATH, check_same_thread=False)
            cur = conn.cursor()
            
            query = "SELECT rowid, telegram_id, username, category, date, description, url, aws_url FROM dataset_backup WHERE aws_url IS NOT NULL"
            params = []
            
            if telegram_id:
                query += " AND telegram_id = ?"
                params.append(telegram_id)
            
            if category:
                query += " AND category = ?"
                params.append(category)
            
            cur.execute(query, params)
            results = cur.fetchall()
            
            conn.close()
            logger.info(f"Found {len(results)} videos with AWS URLs")
            return results
            
        except Exception as e:
            logger.error(f"Failed to retrieve AWS URLs from database: {e}")
            return []
    
    def fetch_video_from_aws(self, aws_url, local_filename=None):
        """Fetch a video from AWS and save it locally temporarily"""
        try:
            if not local_filename:
                # Generate filename from URL
                parsed_url = urlparse(aws_url)
                filename = os.path.basename(parsed_url.path) or f"video_{int(time.time())}.mp4"
                local_filename = os.path.join(self.temp_dir, filename)
            
            logger.info(f"Downloading video from AWS: {aws_url}")
            
            response = requests.get(aws_url, stream=True, timeout=300)
            response.raise_for_status()
            
            with open(local_filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            logger.info(f"Successfully downloaded video to: {local_filename}")
            return local_filename
            
        except Exception as e:
            logger.error(f"Failed to fetch video from AWS: {e}")
            return None
    
    def fetch_videos_for_processing(self, telegram_id=None, category=None):
        """Fetch videos from AWS for processing (e.g., Gemini transcription)"""
        aws_data = self.get_aws_urls_from_db(telegram_id, category)
        
        if not aws_data:
            logger.info("No videos found on AWS")
            return []
        
        fetched_videos = []
        
        for data in aws_data:
            rowid, telegram_id, username, category, date, description, original_url, aws_url = data
            
            # Fetch video from AWS
            local_file = self.fetch_video_from_aws(aws_url)
            
            if local_file:
                video_data = {
                    'rowid': rowid,
                    'telegram_id': telegram_id,
                    'username': username,
                    'category': category,
                    'date': date,
                    'description': description,
                    'original_url': original_url,
                    'aws_url': aws_url,
                    'local_file': local_file
                }
                fetched_videos.append(video_data)
            
        logger.info(f"Successfully fetched {len(fetched_videos)} videos from AWS")
        return fetched_videos
    
    def cleanup_fetched_videos(self, fetched_videos):
        """Clean up locally fetched videos after processing"""
        for video_data in fetched_videos:
            if 'local_file' in video_data:
                self.cleanup_local_file(video_data['local_file'])
    
    def process_all_videos(self):
        """Main function to process all videos"""
        urls_data = self.get_distinct_urls()
        
        if not urls_data:
            logger.info("No URLs found to process")
            return
        
        successful_uploads = 0
        failed_uploads = 0
        
        for url_data in urls_data:
            url, rowid, telegram_id, username, category, date, description = url_data
            
            metadata = {
                'rowid': rowid,
                'telegram_id': telegram_id,
                'username': username,
                'category': category,
                'date': date,
                'description': description
            }
            
            logger.info(f"Processing URL: {url}")
            
            # Download video using Apify
            downloaded_file = self.download_instagram_video_with_apify(url)
            
            if downloaded_file:
                # Upload to AWS
                aws_url = self.upload_to_aws(downloaded_file, url, metadata)
                
                if aws_url:
                    successful_uploads += 1
                    logger.info(f"Successfully processed: {url}")
                else:
                    failed_uploads += 1
                    logger.error(f"Failed to upload: {url}")
                
                # Always cleanup local file
                self.cleanup_local_file(downloaded_file)
            else:
                failed_uploads += 1
                logger.error(f"Failed to download: {url}")
            
            # Add delay between requests
            time.sleep(5)
        
        logger.info(f"Processing complete! Success: {successful_uploads}, Failed: {failed_uploads}")

def main():
    """Main function - automatically processes all videos"""
    try:
        logger.info("Starting Apify Instagram Video Downloader and AWS Uploader")
        downloader = ApifyInstagramDownloader()
        downloader.process_all_videos()
        logger.info("Process completed")
            
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        logger.error("Make sure to add APIFY_API_TOKEN to your .env file")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
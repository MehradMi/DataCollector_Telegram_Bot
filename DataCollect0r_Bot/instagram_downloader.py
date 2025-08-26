import os
import sqlite3
import requests
import logging
import time
import tempfile
from pathlib import Path
from urllib.parse import urlparse
from dotenv import load_dotenv
import yt_dlp
import instaloader
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

class InstagramDownloader:
    def __init__(self):
        self.aws_upload_url = pixoform_aws_upload_url
        self.aws_delete_url = pixoform_aws_delete_url
        self.temp_dir = tempfile.gettempdir()
        
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
    
    def download_instagram_video(self, url, output_path):
        """Download Instagram video using yt-dlp"""
        try:
            # Configure yt-dlp options
            ydl_opts = {
                'outtmpl': output_path,
                'format': 'best[ext=mp4]/best',  # Prefer mp4 format
                'noplaylist': True,
                'extract_flat': False,
                'writeinfojson': False,
                'writesubtitles': False,
                'writeautomaticsub': False,
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # Extract info first to get the actual filename
                info = ydl.extract_info(url, download=False)
                title = info.get('title', 'instagram_video')
                
                # Clean title for filename
                clean_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()
                clean_title = clean_title[:50]  # Limit length
                
                # Update output path with clean title
                final_output = os.path.join(self.temp_dir, f"{clean_title}_{int(time.time())}.%(ext)s")
                ydl_opts['outtmpl'] = final_output
                
                # Download the video
                with yt_dlp.YoutubeDL(ydl_opts) as ydl_download:
                    ydl_download.download([url])
                
                # Find the actual downloaded file
                base_path = final_output.replace('.%(ext)s', '')
                possible_extensions = ['.mp4', '.webm', '.mkv', '.avi']
                
                for ext in possible_extensions:
                    potential_file = base_path + ext
                    if os.path.exists(potential_file):
                        return potential_file
                
                logger.error(f"Downloaded file not found for URL: {url}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to download video from {url}: {e}")
            return None

    def download_instagram_video_instaloader(self, url, output_path):
        match = re.search("instagram\.com/(reel|p)/([A-Za-z0-9_-]+)", url)

        if not match :
            raise ValueError("Invalid url instagram ! ")

        shortcode = match.group(2)

        os.makedirs (output_path , exist_ok=True)

        L = instaloader.Instaloader(
            dirname_pattern=os.path.join(output_dir,"{target}"),
            download_video_thumbnails=False,
            save_metadata=False,
            post_metadata_txt_pattern="" 
        )

        try :
            post = instaloader.Post.from_shortcode(L.context, shortcode)
            L.download_post(post , target=f"{post.owner_username}_{shortcode}")

        except Exception as e :
            logger.error(f"Failed to download video from {url}: {e}")
    
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
                
                response = requests.post(self.aws_upload_url, files=files, timeout=300)  # 5 minute timeout
                
                if response.status_code == 200:
                    result = response.json()
                    aws_url = result.get('file_url') or result.get('url')
                    logger.info(f"✅ Successfully uploaded to AWS: {aws_url}")
                    
                    # Update database with AWS URL
                    self.update_database_with_aws_url(metadata['rowid'], aws_url)
                    
                    return aws_url
                else:
                    logger.error(f"❌ AWS upload failed for {original_url}: {response.status_code} - {response.text}")
                    return None
                    
        except Exception as e:
            logger.error(f"❌ Error uploading {file_path} to AWS: {e}")
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
                logger.info(f"🗑️ Cleaned up local file: {file_path}")
            else:
                logger.warning(f"File not found for cleanup: {file_path}")
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
            
            logger.info(f"✅ Successfully downloaded video to: {local_filename}")
            return local_filename
            
        except Exception as e:
            logger.error(f"❌ Failed to fetch video from AWS: {e}")
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
            
            # Download video
            output_path = os.path.join(self.temp_dir, f"instagram_video_{int(time.time())}.%(ext)s")
            downloaded_file = self.download_instagram_video_instaloader(url, output_path)
            
            if downloaded_file:
                # Upload to AWS
                aws_url = self.upload_to_aws(downloaded_file, url, metadata)
                
                if aws_url:
                    successful_uploads += 1
                    logger.info(f"✅ Successfully processed: {url}")
                else:
                    failed_uploads += 1
                    logger.error(f"❌ Failed to upload: {url}")
                
                # Always cleanup local file
                self.cleanup_local_file(downloaded_file)
            else:
                failed_uploads += 1
                logger.error(f"❌ Failed to download: {url}")
            
            # Add a small delay to be respectful to Instagram's servers
            time.sleep(2)
        
        logger.info(f"Processing complete! ✅ Success: {successful_uploads}, ❌ Failed: {failed_uploads}")

def main():
    """Main function"""
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "upload":
            # Upload all videos to AWS
            logger.info("Starting Instagram Video Downloader and AWS Uploader")
            downloader = InstagramDownloader()
            downloader.process_all_videos()
            logger.info("Upload process completed")
            
        elif command == "fetch":
            # Fetch videos from AWS for processing
            logger.info("Fetching videos from AWS for processing")
            downloader = InstagramDownloader()
            
            # Optional filters
            telegram_id = sys.argv[2] if len(sys.argv) > 2 else None
            category = sys.argv[3] if len(sys.argv) > 3 else None
            
            # Fetch videos
            fetched_videos = downloader.fetch_videos_for_processing(telegram_id, category)
            
            if fetched_videos:
                logger.info(f"Fetched {len(fetched_videos)} videos. Process them here...")
                
                # Example: Print video info (replace with your Gemini processing)
                for video in fetched_videos:
                    logger.info(f"Video available at: {video['local_file']}")
                    logger.info(f"Metadata: {video['description']}")
                    # Here you would call your Gemini API for transcription
                
                # Clean up after processing
                downloader.cleanup_fetched_videos(fetched_videos)
                logger.info("Cleanup completed")
            else:
                logger.info("No videos to fetch")
                
        elif command == "list":
            # List all videos on AWS
            logger.info("Listing all videos on AWS")
            downloader = InstagramDownloader()
            aws_data = downloader.get_aws_urls_from_db()
            
            for data in aws_data:
                rowid, telegram_id, username, category, date, description, original_url, aws_url = data
                print(f"ID: {rowid}, User: {username} ({telegram_id}), Category: {category}")
                print(f"Date: {date}, AWS URL: {aws_url}")
                print(f"Description: {description}")
                print("-" * 50)
        else:
            print("Unknown command. Use 'upload', 'fetch', or 'list'")
    else:
        # Default behavior: upload
        logger.info("Starting Instagram Video Downloader and AWS Uploader")
        downloader = InstagramDownloader()
        downloader.process_all_videos()
        logger.info("Process completed")

if __name__ == "__main__":
    main()
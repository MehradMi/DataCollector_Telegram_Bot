import requests
import os
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PixoformUploader:
    def __init__(self, base_url="https://pixoform.com/api/v1/main"):
        self.base_url = base_url
        self.upload_single_url = f"{base_url}/upload-file"
        self.upload_multiple_url = f"{base_url}/upload-files"
        self.delete_url = f"{base_url}/delete-files"

    def upload_single_file(self, file_path):
        """Upload a single file"""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                logger.error(f"File not found: {file_path}")
                return None

            logger.info(f"Uploading {file_path.name}...")
            
            with open(file_path, 'rb') as f:
                files = {'file': (file_path.name, f, 'video/mp4')}
                response = requests.post(self.upload_single_url, files=files)
            
            response.raise_for_status()
            result = response.json()
            
            logger.info(f"Upload successful: {file_path.name}")
            logger.info(f"Response: {result}")
            
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Upload failed: {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"Error uploading file: {e}")
            return None

    def upload_multiple_files(self, file_paths):
        """Upload multiple files"""
        try:
            files = []
            file_handles = []
            
            for file_path in file_paths:
                file_path = Path(file_path)
                if not file_path.exists():
                    logger.warning(f"File not found, skipping: {file_path}")
                    continue
                
                f = open(file_path, 'rb')
                file_handles.append(f)
                files.append(('files[]', (file_path.name, f, 'video/mp4')))
            
            if not files:
                logger.error("No valid files to upload")
                return None
            
            logger.info(f"Uploading {len(files)} files...")
            
            response = requests.post(self.upload_multiple_url, files=files)
            
            # Close all file handles
            for f in file_handles:
                f.close()
            
            response.raise_for_status()
            result = response.json()
            
            logger.info("Multiple upload successful")
            logger.info(f"Response: {result}")
            
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Multiple upload failed: {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"Error uploading files: {e}")
            return None
        finally:
            # Ensure all files are closed
            for f in file_handles:
                try:
                    f.close()
                except:
                    pass

    def delete_files(self, file_urls):
        """Delete files from server"""
        try:
            logger.info(f"Deleting {len(file_urls)} files...")
            
            data = {"file_urls": file_urls}
            response = requests.post(self.delete_url, json=data)
            
            response.raise_for_status()
            result = response.json()
            
            logger.info("Delete successful")
            logger.info(f"Response: {result}")
            
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Delete failed: {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"Error deleting files: {e}")
            return None

def test_upload():
    """Test function to upload sample files"""
    uploader = PixoformUploader()
    
    # Check if we have any video files in downloads directory
    downloads_dir = Path("downloads")
    if downloads_dir.exists():
        video_files = list(downloads_dir.glob("*.mp4"))
        
        if video_files:
            logger.info(f"Found {len(video_files)} video files to test upload")
            
            # Test single file upload
            first_file = video_files[0]
            result = uploader.upload_single_file(first_file)
            
            if result:
                logger.info("✓ Single file upload test passed")
                
                # If we have multiple files, test multiple upload
                if len(video_files) > 1:
                    multiple_files = video_files[:3]  # Upload max 3 files for testing
                    result_multiple = uploader.upload_multiple_files(multiple_files)
                    
                    if result_multiple:
                        logger.info("✓ Multiple file upload test passed")
                    
                # Test delete (if we got URLs back)
                if isinstance(result, dict) and 'url' in result:
                    delete_result = uploader.delete_files([result['url']])
                    if delete_result:
                        logger.info("✓ Delete test passed")
                        
            else:
                logger.error("✗ Upload test failed")
        else:
            logger.warning("No MP4 files found in downloads directory")
            logger.info("Creating a dummy file for testing...")
            
            # Create a small dummy file for testing
            dummy_file = downloads_dir / "test_video.mp4"
            downloads_dir.mkdir(exist_ok=True)
            
            # Create a minimal MP4 file (just for testing - not a real video)
            dummy_content = b'\x00\x00\x00\x20ftypmp42\x00\x00\x00\x00mp42isom'
            with open(dummy_file, 'wb') as f:
                f.write(dummy_content * 1000)  # Make it a bit bigger
            
            logger.info(f"Created dummy file: {dummy_file}")
            result = uploader.upload_single_file(dummy_file)
            
            if result:
                logger.info("✓ Dummy file upload test passed")
            else:
                logger.error("✗ Dummy file upload test failed")
    else:
        logger.error("Downloads directory not found")

def main():
    logger.info("Starting Pixoform upload test...")
    test_upload()
    logger.info("Test complete!")

if __name__ == "__main__":
    main()
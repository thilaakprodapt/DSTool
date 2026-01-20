from datetime import timedelta
from urllib.parse import urlparse
from google.cloud import storage as gcs_storage
from app.core.config import credentials, PROJECT_ID

def refresh_signed_urls_in_data(data, expiration_hours=24):
    """
    Recursively traverse the data structure and regenerate signed URLs for GCS images.
    This ensures that URLs retrieved from the database are always fresh.
    
    Args:
        data: Dictionary or list containing the analysis data
        expiration_hours: How long the new signed URLs should be valid
        
    Returns:
        Modified data with fresh signed URLs
    """
    from urllib.parse import urlparse
    from google.cloud import storage as gcs_storage
    
    # Initialize storage client (reuse credentials)
    storage_client = gcs_storage.Client(credentials=credentials, project=PROJECT_ID)
    
    def is_gcs_signed_url(url):
        """Check if a URL is a GCS signed URL"""
        if not isinstance(url, str):
            return False
        return 'storage.googleapis.com' in url and 'X-Goog-Algorithm' in url
    
    def extract_gcs_path_from_url(url):
        """Extract bucket and blob path from a signed URL"""
        try:
            # Parse URL: https://storage.googleapis.com/bucket/path/to/file.png?X-Goog-...
            parsed = urlparse(url)
            path_parts = parsed.path.lstrip('/').split('/', 1)
            if len(path_parts) == 2:
                return path_parts[0], path_parts[1]  # bucket, blob_path
        except:
            pass
        return None, None
    
    def regenerate_url(url):
        """Regenerate a signed URL"""
        try:
            bucket_name, blob_path = extract_gcs_path_from_url(url)
            if not bucket_name or not blob_path:
                return url  # Return original if can't parse
            
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            
            # Check if blob exists
            if not blob.exists():
                print(f"⚠️ Blob not found: gs://{bucket_name}/{blob_path}")
                return url
            
            # Generate new signed URL
            new_signed_url = blob.generate_signed_url(
                version="v4",
                expiration=timedelta(hours=expiration_hours),
                method="GET"
            )
            print(f"✓ Refreshed URL for: {blob_path}")
            return new_signed_url
            
        except Exception as e:
            print(f"⚠️ Failed to regenerate URL: {e}")
            return url  # Return original on error
    
    def traverse(obj):
        """Recursively traverse and update URLs"""
        if isinstance(obj, dict):
            return {k: traverse(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [traverse(item) for item in obj]
        elif isinstance(obj, str) and is_gcs_signed_url(obj):
            return regenerate_url(obj)
        else:
            return obj
    
    return traverse(data)
"""
Alternative HDFS upload script using Python hdfs library.
Install with: pip install hdfs
"""
try:
    from hdfs import InsecureClient
    HDFS_AVAILABLE = True
except ImportError:
    HDFS_AVAILABLE = False
    print("⚠️  hdfs library not installed. Install with: pip install hdfs")


def upload_csv_to_hdfs_hdfs_lib(local_file_path: str, hdfs_path: str = "/data/lastfm_data.csv"):
    """
    Upload CSV to HDFS using Python hdfs library.
    
    Args:
        local_file_path: Path to local CSV file
        hdfs_path: HDFS destination path
    """
    if not HDFS_AVAILABLE:
        print("Please install hdfs library: pip install hdfs")
        return False
    
    from pathlib import Path
    
    local_file = Path(local_file_path)
    if not local_file.exists():
        print(f"❌ File not found: {local_file_path}")
        return False
    
    # Connect to HDFS
    # Namenode web UI is on port 9870, but we need the RPC port 9000
    # Try webhdfs on port 9870 first
    try:
        client = InsecureClient('http://localhost:9870', user='root')
        print(f"✓ Connected to HDFS")
    except Exception as e:
        print(f"❌ Could not connect to HDFS: {e}")
        print("Make sure HDFS containers are running and accessible")
        return False
    
    # Create directory if needed
    hdfs_dir = "/".join(hdfs_path.split("/")[:-1])
    try:
        client.makedirs(hdfs_dir)
        print(f"✓ Created/verified directory: {hdfs_dir}")
    except Exception as e:
        print(f"Warning: {e}")
    
    # Upload file
    try:
        print(f"Uploading {local_file_path} to {hdfs_path}...")
        client.upload(hdfs_path, str(local_file.absolute()))
        print(f"✓ Successfully uploaded to {hdfs_path}")
        
        # Verify
        status = client.status(hdfs_path)
        print(f"\nFile info:")
        print(f"  Size: {status['length']} bytes")
        print(f"  Path: {hdfs_path}")
        return True
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False


if __name__ == "__main__":
    csv_file = r"data\lastfm_data.csv"
    hdfs_destination = "/data/lastfm_data.csv"
    
    upload_csv_to_hdfs_hdfs_lib(csv_file, hdfs_destination)




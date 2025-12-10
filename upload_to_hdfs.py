import subprocess
import os
from pathlib import Path

def upload_csv_to_hdfs(local_file_path: str, hdfs_path: str = "/data/lastfm_data.csv"):
    """
    Upload a CSV file to HDFS using docker exec.
    
    Args:
        local_file_path: Path to the local CSV file
        hdfs_path: HDFS destination path (default: /data/lastfm_data.csv)
    """
    local_file = Path(local_file_path)
    
    if not local_file.exists():
        raise FileNotFoundError(f"Local file not found: {local_file_path}")
    
    print(f"Uploading {local_file_path} to HDFS: {hdfs_path}")
    
    # First, copy the file into the namenode container
    print("Step 1: Copying file to namenode container...")
    copy_cmd = [
        "docker", "cp", 
        str(local_file.absolute()),
        f"namenode:/tmp/{local_file.name}"
    ]
    
    try:
        result = subprocess.run(copy_cmd, check=True, capture_output=True, text=True)
        print("✓ File copied to container")
    except subprocess.CalledProcessError as e:
        print(f"Error copying file to container: {e.stderr}")
        raise
    
    # Create HDFS directory if it doesn't exist
    print("Step 2: Creating HDFS directory...")
    mkdir_cmd = [
        "docker", "exec", "namenode",
        "hdfs", "dfs", "-mkdir", "-p", "/data"
    ]
    
    try:
        subprocess.run(mkdir_cmd, check=True, capture_output=True, text=True)
        print("✓ HDFS directory created/verified")
    except subprocess.CalledProcessError as e:
        # Directory might already exist, that's okay
        if "already exists" not in e.stderr.lower():
            print(f"Warning: {e.stderr}")
    
    # Upload file to HDFS
    print("Step 3: Uploading file to HDFS...")
    upload_cmd = [
        "docker", "exec", "namenode",
        "hdfs", "dfs", "-put",
        f"/tmp/{local_file.name}",
        hdfs_path
    ]
    
    try:
        result = subprocess.run(upload_cmd, check=True, capture_output=True, text=True)
        print(f"✓ File uploaded successfully to {hdfs_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error uploading to HDFS: {e.stderr}")
        raise
    
    # Verify the file was uploaded
    print("Step 4: Verifying upload...")
    verify_cmd = [
        "docker", "exec", "namenode",
        "hdfs", "dfs", "-ls", hdfs_path
    ]
    
    try:
        result = subprocess.run(verify_cmd, check=True, capture_output=True, text=True)
        print("✓ File verified in HDFS")
        print(f"\nHDFS file info:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Warning: Could not verify file: {e.stderr}")
    
    # Clean up temporary file in container
    print("Step 5: Cleaning up temporary file...")
    cleanup_cmd = [
        "docker", "exec", "namenode",
        "rm", f"/tmp/{local_file.name}"
    ]
    
    try:
        subprocess.run(cleanup_cmd, check=True, capture_output=True, text=True)
        print("✓ Cleanup complete")
    except subprocess.CalledProcessError:
        pass  # Ignore cleanup errors
    
    print(f"\n✅ Successfully uploaded {local_file_path} to HDFS at {hdfs_path}")
    return hdfs_path


def check_hdfs_connection():
    """Check if HDFS is accessible via docker."""
    print("Checking HDFS connection...")
    
    # Check if namenode container is running
    check_cmd = ["docker", "ps", "--filter", "name=namenode", "--format", "{{.Names}}"]
    
    try:
        result = subprocess.run(check_cmd, check=True, capture_output=True, text=True)
        if "namenode" not in result.stdout:
            print("❌ Namenode container is not running!")
            print("Please start the containers with: docker-compose up -d")
            return False
        print("✓ Namenode container is running")
    except subprocess.CalledProcessError:
        print("❌ Could not check docker containers")
        return False
    
    # Test HDFS connection
    test_cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-ls", "/"]
    
    try:
        result = subprocess.run(test_cmd, check=True, capture_output=True, text=True, timeout=10)
        print("✓ HDFS is accessible")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ HDFS connection failed: {e.stderr}")
        return False
    except subprocess.TimeoutExpired:
        print("❌ HDFS connection timeout")
        return False


if __name__ == "__main__":
    # Check HDFS connection first
    if not check_hdfs_connection():
        print("\n⚠️  Please ensure Docker containers are running:")
        print("   docker-compose up -d")
        print("\nIf you're having Docker issues, you may need to:")
        print("   1. Restart Docker Desktop")
        print("   2. Check Docker API version compatibility")
        exit(1)
    
    # Upload the CSV file
    csv_file = r"data\lastfm_data.csv"
    hdfs_destination = "/data/lastfm_data.csv"
    
    try:
        upload_csv_to_hdfs(csv_file, hdfs_destination)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        exit(1)




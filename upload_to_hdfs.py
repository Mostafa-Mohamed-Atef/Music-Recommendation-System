import subprocess
from pathlib import Path

def run_cmd(cmd):
    """Run a command and return (success, stdout, stderr)."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return True, result.stdout.strip(), result.stderr.strip()
    except subprocess.CalledProcessError as e:
        return False, e.stdout.strip(), e.stderr.strip()


def file_exists_in_container(container_path):
    """Check if a file exists inside the namenode container."""
    cmd = ["docker", "exec", "namenode", "test", "-f", container_path]
    success, _, _ = run_cmd(cmd)
    return success


def hdfs_path_exists(hdfs_path):
    """Check if a file or directory exists in HDFS."""
    cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-test", "-e", hdfs_path]
    success, _, _ = run_cmd(cmd)
    return success


def upload_csv_to_hdfs(local_file_path: str, hdfs_path: str = "/data/lastfm_data.csv"):
    local_file = Path(local_file_path)

    if not local_file.exists():
        raise FileNotFoundError(f"Local file not found: {local_file_path}")

    container_tmp_path = f"/tmp/{local_file.name}"

    print(f"\n=== Uploading {local_file_path} → HDFS:{hdfs_path} ===\n")

    # -------------------------------------------------------
    # Step 1 — Copy to namenode container (skip if exists)
    # -------------------------------------------------------
    print("Step 1: Checking file inside namenode container…")

    if file_exists_in_container(container_tmp_path):
        print(f"✓ File already exists in container: {container_tmp_path} (skipping copy)")
    else:
        print("→ File not found in container, copying 2GB file (this may take time)…")
        copy_cmd = ["docker", "cp", str(local_file.absolute()), f"namenode:{container_tmp_path}"]
        success, out, err = run_cmd(copy_cmd)
        if not success:
            raise RuntimeError(f"Error copying file: {err}")
        print("✓ File copied into container")

    # -------------------------------------------------------
    # Step 2 — Create HDFS directory if needed
    # -------------------------------------------------------
    print("\nStep 2: Ensuring HDFS directory exists /data…")

    if hdfs_path_exists("/data"):
        print("✓ HDFS directory already exists (skipping mkdir)")
    else:
        print("→ Creating HDFS directory /data")
        mkdir_cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/data"]
        success, out, err = run_cmd(mkdir_cmd)
        if not success:
            raise RuntimeError(f"Failed to create HDFS directory: {err}")
        print("✓ HDFS directory created")

    # -------------------------------------------------------
    # Step 3 — Upload to HDFS (skip if exists)
    # -------------------------------------------------------
    print("\nStep 3: Uploading to HDFS…")

    if hdfs_path_exists(hdfs_path):
        print(f"✓ HDFS file already exists: {hdfs_path} (skipping upload)")
    else:
        print("→ Uploading file to HDFS (this may take 10–40 seconds)…")
        upload_cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-put", container_tmp_path, hdfs_path]
        success, out, err = run_cmd(upload_cmd)
        if not success:
            raise RuntimeError(f"Error uploading to HDFS: {err}")
        print("✓ File uploaded to HDFS")

    # -------------------------------------------------------
    # Step 4 — Verify upload
    # -------------------------------------------------------
    print("\nStep 4: Verifying HDFS file…")
    ls_cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-ls", hdfs_path]
    success, out, err = run_cmd(ls_cmd)
    if not success:
        print(f"⚠️ Could not verify file in HDFS: {err}")
    else:
        print("✓ File verified in HDFS:")
        print(out)

    print("\n🎉 DONE — Upload process completed successfully!\n")


def check_hdfs_connection():
    print("Checking HDFS connection...")

    # Check namenode container
    ps_cmd = ["docker", "ps", "--filter", "name=namenode", "--format", "{{.Names}}"]
    success, out, err = run_cmd(ps_cmd)

    if "namenode" not in out:
        print("❌ Namenode container not running. Start with: docker-compose up -d")
        return False

    print("✓ Namenode container is running")

    # Check HDFS
    test_cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-ls", "/"]
    success, out, err = run_cmd(test_cmd)

    if not success:
        print("❌ HDFS not accessible:", err)
        return False

    print("✓ HDFS is accessible")
    return True


if __name__ == "__main__":
    if not check_hdfs_connection():
        exit(1)

    csv_file = "data/lastfm_data.csv"
    hdfs_destination = "/data/lastfm_data.csv"

    try:
        upload_csv_to_hdfs(csv_file, hdfs_destination)
    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        exit(1)

# Manual HDFS Upload Instructions

If Docker is working, you can upload `lastfm_data.csv` to HDFS using these commands:

## Option 1: Using the Python Script (Recommended)
```bash
python upload_to_hdfs.py
```

## Option 2: Manual Docker Commands

1. **Copy file to namenode container:**
   ```bash
   docker cp data\lastfm_data.csv namenode:/tmp/lastfm_data.csv
   ```

2. **Create HDFS directory:**
   ```bash
   docker exec namenode hdfs dfs -mkdir -p /data
   ```

3. **Upload to HDFS:**
   ```bash
   docker exec namenode hdfs dfs -put /tmp/lastfm_data.csv /data/lastfm_data.csv
   ```

4. **Verify upload:**
   ```bash
   docker exec namenode hdfs dfs -ls /data/lastfm_data.csv
   ```

5. **View file info:**
   ```bash
   docker exec namenode hdfs dfs -ls -h /data/lastfm_data.csv
   ```

## Troubleshooting Docker Issues

If you're getting Docker API version errors:

1. **Restart Docker Desktop** - This often resolves API version mismatches
2. **Update Docker Desktop** to the latest version
3. **Check Docker API version:**
   ```bash
   docker version
   ```

4. **Try starting containers individually:**
   ```bash
   docker-compose up namenode -d
   docker-compose up datanode -d
   ```

5. **Check container logs:**
   ```bash
   docker logs namenode
   docker logs datanode
   ```

## Alternative: Using Python hdfs Library

If Docker continues to have issues, you can install and use the `hdfs` library:

```bash
pip install hdfs
```

Then use:
```python
from hdfs import InsecureClient

client = InsecureClient('http://localhost:9870', user='root')
client.upload('/data', 'data/lastfm_data.csv')
```




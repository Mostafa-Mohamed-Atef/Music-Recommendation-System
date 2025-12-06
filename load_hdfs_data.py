import os
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict
import glob

class HDFSDataLoader:
    def __init__(self, base_path: str):
        """Initialize the HDFS data loader with base path."""
        self.base_path = Path(base_path)
        self.data = []
        
    def load_all_json_files(self) -> List[Dict]:
        """
        Load all JSON files recursively from the directory structure.
        Returns a list of dictionaries containing all data.
        """
        json_pattern = str(self.base_path / "**/*.json")
        json_files = glob.glob(json_pattern, recursive=True)
        
        print(f"Found {len(json_files)} JSON files to load...")
        
        for idx, file_path in enumerate(json_files):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.data.append(data)
                    
                if (idx + 1) % 10000 == 0:
                    print(f"Loaded {idx + 1}/{len(json_files)} files...")
                    
            except Exception as e:
                print(f"Error loading {file_path}: {e}")
        
        print(f"Successfully loaded {len(self.data)} records")
        return self.data
    
    def to_dataframe(self) -> pd.DataFrame:
        """Convert loaded data to a pandas DataFrame."""
        if not self.data:
            self.load_all_json_files()
        
        return pd.DataFrame(self.data)
    
    def get_statistics(self) -> Dict:
        """Get basic statistics about loaded data."""
        if not self.data:
            print("No data loaded. Loading data first...")
            self.load_all_json_files()
        
        return {
            'total_records': len(self.data),
            'columns': list(self.data[0].keys()) if self.data else [],
            'memory_usage_mb': sum(len(json.dumps(record)) for record in self.data) / (1024 * 1024)
        }


def main():
    # Path to your HDFS data
    hdfs_path = r"e:\Dev\Ai\music recommendation\lastfm_train\lastfm_train"
    
    # Initialize loader
    loader = HDFSDataLoader(hdfs_path)
    
    # Load all data
    print("Starting HDFS data load...")
    data = loader.load_all_json_files()
    
    # Display statistics
    stats = loader.get_statistics()
    print("\n" + "="*50)
    print("Data Statistics:")
    print("="*50)
    print(f"Total Records: {stats['total_records']}")
    print(f"Columns: {stats['columns']}")
    print(f"Memory Usage: {stats['memory_usage_mb']:.2f} MB")
    
    # Convert to DataFrame
    print("\nConverting to DataFrame...")
    df = loader.to_dataframe()
    
    print("\nDataFrame Info:")
    print(f"Shape: {df.shape}")
    print("\nFirst few records:")
    print(df.head())
    
    # Optional: Save to CSV
    output_path = r"e:\Dev\Ai\music recommendation\lastfm_data.csv"
    print(f"\nSaving data to {output_path}...")
    
    # Flatten nested structures for CSV export
    df_flat = df.copy()
    df_flat['similars'] = df_flat['similars'].apply(lambda x: str(x) if x else '')
    df_flat['tags'] = df_flat['tags'].apply(lambda x: str(x) if x else '')
    df_flat.to_csv(output_path, index=False)
    
    print("Data saved successfully!")
    
    return loader, df


if __name__ == "__main__":
    loader, df = main()

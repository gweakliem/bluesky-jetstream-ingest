import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Union

import pandas as pd
from google.cloud import storage

class ParquetWriter:
    def __init__(self, output_path: Union[str, Path]):
        """Initialize the Parquet writer.
        
        Args:
            output_path: Local directory or GCS bucket path for output
        """
        self.output_path = str(output_path)
        self.is_gcs = self.output_path.startswith('gs://')
        
        if self.is_gcs:
            self.gcs_client = storage.Client()
            self.bucket_name = self.output_path.split('/')[2]
            self.gcs_prefix = '/'.join(self.output_path.split('/')[3:])
            self.bucket = self.gcs_client.bucket(self.bucket_name)
        else:
            os.makedirs(self.output_path, exist_ok=True)

    def write_batch(self, messages: List[Dict]):
        """Write a batch of messages to Parquet.
        
        Args:
            messages: List of message dictionaries with receivedTimestamp and payload
        """
        if not messages:
            return

        df = pd.DataFrame(messages)
        
        # Generate filename with timestamp
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        filename = f"jetstream_{timestamp}.parquet"
        
        if self.is_gcs:
            # Write to temporary file then upload to GCS
            temp_path = f"/tmp/{filename}"
            df.to_parquet(temp_path, index=False)
            
            blob_path = os.path.join(self.gcs_prefix, filename)
            blob = self.bucket.blob(blob_path)
            blob.upload_from_filename(temp_path)
            os.remove(temp_path)
        else:
            # Write directly to local filesystem
            output_file = os.path.join(self.output_path, filename)
            df.to_parquet(output_file, index=False)

import pandas as pd
from typing import List, Dict
import os

def write_parquet(records: List[Dict], output_path: str):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df = pd.DataFrame(records)
    df.to_parquet(output_path, index=False)

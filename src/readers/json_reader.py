import json
from typing import List, Dict

def read_json_file(path: str) -> List[Dict]:
    """
    Reads a JSON file containing one JSON object per line.
    """
    records = []
    with open(path, "r") as f:
        for line in f:
            records.append(json.loads(line))
    return records

import json
from readers.json_reader import read_json_file
from validation.schema_validator import validate_record
from writers.parquet_writer import write_parquet

SCHEMA_PATH = "schemas/generic_event.json"
INPUT_PATH = "data/input/binance_events.json"
OUTPUT_PATH = "data/output/bronze/binance.parquet"

def load_schema():
    with open(SCHEMA_PATH) as f:
        return json.load(f)

def run_pipeline():
    schema = load_schema()
    records = read_json_file(INPUT_PATH)
    
    valid_records = []
    invalid_records = []
    
    for record in records:
        is_valid,error = validate_record(record,schema)
        if is_valid:
            valid_records.append(record)
        else:
            invalid_records.append({"record":record,"error":error})
            
    
    write_parquet(valid_records, OUTPUT_PATH)

    print(f"Valid records: {len(valid_records)}")
    print(f"Invalid records: {len(invalid_records)}")

if __name__ == "__main__":
    run_pipeline()


from jsonschema import validate, ValidationError
from typing import Dict, Tuple

def validate_record(record: Dict, schema: Dict) -> Tuple[bool, str]:
    """
    Validates a record against a JSON schema.
    Returns (is_valid, error_message)
    """
    try:
        validate(instance=record, schema=schema)
        return True, ""
    except ValidationError as e:
        return False, str(e)

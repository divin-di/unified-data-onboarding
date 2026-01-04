def check_schema_compatibility(old_schema, new_schema):
    errors = []

    for col, dtype in old_schema.items():
        if col not in new_schema:
            errors.append(f"Column removed: {col}")
        elif new_schema[col] != dtype:
            errors.append(f"Type change: {col}")

    return errors

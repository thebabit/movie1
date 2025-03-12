def send(record):
    global batch_records, record_count, record_schema, is_dict_mode

    if record_schema is None:
        if isinstance(record, dict):
            record_schema = list(record.keys())
            is_dict_mode = True
        elif isinstance(record, list) and all(isinstance(col, str) for col in record):
            record_schema = record
            is_dict_mode = False
            print(f"Detected schema from first row: {record_schema}")
            return  # Don’t treat this row as data

        elif isinstance(record, list):
            record_schema = [f"col_{i}" for i in range(len(record))]
            is_dict_mode = False

    if not is_dict_mode and isinstance(record, list):
        record = dict(zip(record_schema, record))

    batch_records.append(record)
    record_count += 1

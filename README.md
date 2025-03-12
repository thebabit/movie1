def send(record):
    global batch_records, record_count, record_schema

    if record_schema is None:
        # First record = schema
        if isinstance(record, dict):
            record_schema = list(record.keys())
        elif isinstance(record, list):
            record_schema = [f"col_{i}" for i in range(len(record))]
        else:
            raise ValueError("First record must be a list or dict")
        print(f"Schema captured: {record_schema}")
        return  # Skip saving this record

    # Convert list to dict using schema
    if isinstance(record, list):
        record = dict(zip(record_schema, record))

    batch_records.append(record)
    record_count += 1

    if len(batch_records) >= PARTITION_SIZE:
        flush_partition()

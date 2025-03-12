import pandas as pd
import os
import datetime

# Config
BASE_FILE_NAME = "output"
PARTITION_SIZE = 1000
DELIMITER = "|"  # Not used anymore, but kept for compatibility
PARQUET_COMPRESSION = "snappy"

# State
file_index = 1
record_count = 0
batch_records = []
record_schema = None

def get_partitioned_filename():
    return f"{BASE_FILE_NAME}_{file_index}.parquet"

def get_today_filename(extension):
    return f"{datetime.datetime.now().strftime('%Y%m%d')}.{extension}"

def write_tok_file():
    now = datetime.datetime.now()
    full_timestamp = now.strftime("%Y%m%d%H%M%S")
    tok_filename = get_today_filename("tok")

    with open(tok_filename, "w", encoding="utf-8") as f:
        f.write(f"{full_timestamp}\n")
        f.write(f"record_count: {record_count}")

    print(f"!!!!! TOK FILE CREATED: {tok_filename} !!!!!")

def start():
    global file_index, record_count, batch_records, record_schema
    file_index = 1
    record_count = 0
    batch_records = []
    record_schema = None
    print("!!!!! OUTPUT STARTED - Parquet with Snappy Compression !!!!!")

def send(record):
    global batch_records, record_count, record_schema

    if record_schema is None:
        if isinstance(record, dict):
            record_schema = list(record.keys())
        elif isinstance(record, list):
            record_schema = [f"col_{i}" for i in range(len(record))]
        else:
            raise ValueError("First record must be a list or dict")
        print(f"Schema captured: {record_schema}")
        return  # Skip this as data

    if isinstance(record, list):
        record = dict(zip(record_schema, record))

    batch_records.append(record)
    record_count += 1

    if len(batch_records) >= PARTITION_SIZE:
        flush_partition()

def flush_partition():
    global batch_records, file_index

    if not batch_records:
        return

    file_name = get_partitioned_filename()
    df = pd.DataFrame(batch_records, columns=record_schema)

    df.to_parquet(
        file_name,
        index=False,
        compression=PARQUET_COMPRESSION,
        engine="pyarrow"
    )

    print(f"!!!!! PARQUET WRITTEN: {file_name} with {len(batch_records)} records !!!!!")

    file_index += 1
    batch_records = []

def close():
    flush_partition()
    write_tok_file()

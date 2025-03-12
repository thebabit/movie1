import pandas as pd
import os
import gzip
import datetime

# Config
BASE_FILE_NAME = "output"
PARTITION_SIZE = 1000
DELIMITER = "|"

# State
file_index = 1
record_count = 0
write_header = True
batch_records = []
record_schema = None

def get_partitioned_filename():
    return f"{BASE_FILE_NAME}_{file_index}.csv.gz"

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

def write_txt_file():
    txt_filename = get_today_filename("txt")

    if not batch_records:
        schema_line = "no schema"
    else:
        df = pd.DataFrame(batch_records)
        schema_line = DELIMITER.join(df.columns.astype(str))

    with open(txt_filename, "w", encoding="utf-8") as f:
        f.write(schema_line)

    print(f"!!!!! TXT FILE CREATED: {txt_filename} !!!!!")

def start():
    global file_index, record_count, write_header, batch_records, record_schema
    file_index = 1
    record_count = 0
    write_header = True
    batch_records = []
    record_schema = None
    print(f"!!!!! OUTPUT STARTED - Delimiter '{DELIMITER}' !!!!!")

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
        return  # First record is schema only

    if isinstance(record, list):
        record = dict(zip(record_schema, record))

    batch_records.append(record)
    record_count += 1

    if len(batch_records) >= PARTITION_SIZE:
        flush_partition()

def flush_partition():
    global batch_records, file_index, write_header

    if not batch_records:
        return

    file_name = get_partitioned_filename()
    df = pd.DataFrame(batch_records, columns=record_schema)

    df.to_csv(
        file_name,
        index=False,
        header=write_header,
        sep=DELIMITER,
        compression="gzip",
        mode="w"
    )

    print(f"!!!!! OUTPUT COMPLETED - File {file_name} written with {len(batch_records)} records !!!!!")

    file_index += 1
    write_header = False
    batch_records = []

def close():
    flush_partition()
    write_tok_file()
    write_txt_file()

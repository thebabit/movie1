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
is_dict_mode = False

def get_partitioned_filename():
    return f"{BASE_FILE_NAME}_{file_index}.csv.gz"

def get_today_filename(extension):
    today = datetime.datetime.now().strftime("%Y%m%d")
    return f"{today}.{extension}"

def write_auxiliary_files():
    now = datetime.datetime.now()
    date_str = now.strftime("%Y%m%d")
    full_timestamp = now.strftime("%Y%m%d%H%M%S")

    tok_filename = f"{date_str}.tok"
    txt_filename = f"{date_str}.txt"

    with open(tok_filename, "w", encoding="utf-8") as f:
        f.write(f"{full_timestamp}\n")
        f.write(f"record_count: {record_count}")

    with open(txt_filename, "w", encoding="utf-8") as f:
        if record_schema:
            f.write(DELIMITER.join(record_schema))
        else:
            f.write("no schema")

    print(f"!!!!! AUX FILES CREATED: {tok_filename}, {txt_filename} !!!!!")

def start():
    global file_index, record_count, write_header, batch_records
    file_index = 1
    record_count = 0
    write_header = True
    batch_records = []
    print(f"!!!!! OUTPUT STARTED - Delimiter '{DELIMITER}' !!!!!")

def send(record):
    global batch_records, record_count, record_schema, is_dict_mode

    if record_schema is None:
        if isinstance(record, dict):
            record_schema = list(record.keys())
            is_dict_mode = True
        elif isinstance(record, list):
            record_schema = [f"col_{i}" for i in range(len(record))]
            is_dict_mode = False

    if not is_dict_mode and isinstance(record, list):
        # Convert list to dict using schema
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
    df = pd.DataFrame(batch_records)

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
    write_auxiliary_files()

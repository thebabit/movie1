
import gzip
import os
import pandas as pd

BASE_FILE_NAME = "dmg_cust_output.txt.gz"
DELIMITER = "\t"
PATH_PREFIX = ""

file_path = os.path.join(PATH_PREFIX, BASE_FILE_NAME)
file_exists = False
header_written = False
saved_header = None

def start():
    global file_exists, header_written
    file_exists = os.path.exists(file_path)
    header_written = file_exists
    print(f"===== OUTPUT STARTED - File exists: {file_exists} =====")

def send(record_df):
    global header_written, saved_header

    if not isinstance(record_df, pd.DataFrame):
        raise ValueError("Expected a pandas DataFrame as input to 'send()'.")

    header = list(record_df.columns)
    lines = record_df.to_csv(index=False, sep=DELIMITER, header=not header_written, line_terminator='\n')

    with gzip.open(file_path, "at", encoding="utf-8") as f:
        f.write(lines)

    if not header_written:
        saved_header = [str(col).lower() for col in header]
        header_written = True
        print(f"# Header written to {BASE_FILE_NAME}")
        print(f"# Header content: {DELIMITER.join(saved_header)}")

def close():
    print(f"===== OUTPUT COMPLETED - Data appended to {BASE_FILE_NAME} =====")

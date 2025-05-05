
import gzip
import os

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

def send(record):
    global header_written, saved_header

    line = DELIMITER.join(str(field) for field in record) + "\n"

    with gzip.open(file_path, "at", encoding="utf-8") as f:
        if not header_written:
            f.write(line)
            saved_header = [str(field).lower() for field in record]
            header_written = True
            print(f"# Header written to {BASE_FILE_NAME}")
            print(f"# Header content: {DELIMITER.join(saved_header)}")
        else:
            record_lower = [str(field).lower() for field in record]
            if saved_header and record_lower == saved_header:
                print("# Duplicate header detected — skipped.")
                return
            if isinstance(record, (list, tuple)) and len(record) > 0:
                f.write(line)

def close():
    print(f"===== OUTPUT COMPLETED - Data appended to {BASE_FILE_NAME} =====")

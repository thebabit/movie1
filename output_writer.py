
import gzip
import os

BASE_FILE_NAME = "dmg_cust_output.txt.gz"
DELIMITER = "\t"
PATH_PREFIX = ""

file_path = os.path.join(PATH_PREFIX, BASE_FILE_NAME)
file_exists = False
header_written = False

def start():
    global file_exists, header_written
    file_exists = os.path.exists(file_path)
    header_written = file_exists
    print(f"===== OUTPUT STARTED - File exists: {file_exists} =====")

def send(record):
    global header_written

    line = DELIMITER.join(str(field) for field in record) + "\n"

    with gzip.open(file_path, "at", encoding="utf-8") as f:
        # Nếu file chưa có header, ghi dòng đầu tiên là header
        if not header_written:
            f.write(line)
            header_written = True
            print(f"Header written to {BASE_FILE_NAME}")
        else:
            # Nếu header đã ghi rồi → bỏ dòng đầu tiên (header), chỉ ghi data
            if record != [] and record[0].lower() not in ["id", "name", "age"]:  # tùy theo format header bạn dùng
                f.write(line)

def close():
    print(f"===== OUTPUT COMPLETED - Data appended to {BASE_FILE_NAME} =====")

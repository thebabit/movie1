import pandas as pd
import os
import gzip
import datetime

# Global variables
BASE_FILE_NAME = "output"
PARTITION_SIZE = 1000  # Number of records per partition
file_index = 1
record_count = 0  # Total number of records written
write_header = True  # Headers only in the first partition
DELIMITER = "|"  # Fixed delimiter

def get_partitioned_filename():
    """Generate a new gzip CSV filename for each partition."""
    return f"{BASE_FILE_NAME}_{file_index}.csv.gz"

def get_today_filename(extension):
    """Generate a filename with today's date for .tok and .txt files."""
    today = datetime.datetime.now().strftime("%Y%m%d")
    return f"{today}.{extension}"

def write_auxiliary_files():
    """Write a .tok file with the total record count and a .txt file with 'hello work'."""
    tok_filename = get_today_filename("tok")
    txt_filename = get_today_filename("txt")

    with open(tok_filename, "w", encoding="utf-8") as f:
        f.write(f"hello {record_count}")  # Write total records count in .tok file

    with open(txt_filename, "w", encoding="utf-8") as f:
        f.write("hello work")  # Fixed content for .txt file

    print(f"!!!!! AUX FILES CREATED: {tok_filename} (hello {record_count}), {txt_filename} !!!!!")

def start():
    """Initialize partitioning and set up global variables."""
    global record_count, write_header, file_index
    record_count = 0
    print(f"!!!!! OUTPUT STARTED - Writing partition {file_index} with delimiter '{DELIMITER}' !!!!!")

def send(record):
    """Write a single record directly to a new partitioned file."""
    global record_count, file_index, write_header

    file_name = get_partitioned_filename()

    # If new partition, remove existing file to ensure fresh write
    if record_count == 0 and os.path.exists(file_name):
        os.remove(file_name)

    # Convert record to DataFrame
    df = pd.DataFrame([record])

    # Write immediately to the CSV file (without buffering)
    df.to_csv(
        file_name,
        index=False,
        header=write_header if record_count == 0 else False,  # Headers only in first partition
        sep=DELIMITER,
        compression="gzip",
        mode="a"
    )

    print(f"Record written to {file_name}: {record}")

    record_count += 1  # Increment total record count

    # If partition size is reached, move to the next partition
    if record_count >= PARTITION_SIZE:
        close()
        file_index += 1  # Move to the next partition
        start()  # Reset for next partition

def close():
    """Finalize writing, create auxiliary files, and log completion."""
    print(f"!!!!! OUTPUT COMPLETED - File {get_partitioned_filename()} closed !!!!!")
    write_auxiliary_files()  # Create .tok and .txt files








hhhhhhhhh






import pandas as pd
import os
import gzip
import datetime

# Global variables
BASE_FILE_NAME = "output"
PARTITION_SIZE = 1000  # Number of records per partition
file_index = 1
record_count = 0  # Total number of records written
write_header = True  # Headers only in the first partition
DELIMITER = "|"  # Fixed delimiter
batch_records = []  # Store batch of records before writing

def get_partitioned_filename():
    """Generate a new gzip CSV filename for each partition."""
    return f"{BASE_FILE_NAME}_{file_index}.csv.gz"

def get_today_filename(extension):
    """Generate a filename with today's date for .tok and .txt files."""
    today = datetime.datetime.now().strftime("%Y%m%d")
    return f"{today}.{extension}"

def write_auxiliary_files():
    """Write a .tok file with the total record count and a .txt file with 'hello work'."""
    tok_filename = get_today_filename("tok")
    txt_filename = get_today_filename("txt")

    with open(tok_filename, "w", encoding="utf-8") as f:
        f.write(f"hello {record_count}")  # Write total records count in .tok file

    with open(txt_filename, "w", encoding="utf-8") as f:
        f.write("hello work")  # Fixed content for .txt file

    print(f"!!!!! AUX FILES CREATED: {tok_filename} (hello {record_count}), {txt_filename} !!!!!")

def start():
    """Initialize partitioning and set up global variables."""
    global record_count, write_header, file_index, batch_records
    record_count = 0
    batch_records = []  # Reset batch storage
    print(f"!!!!! OUTPUT STARTED - Writing partition {file_index} with delimiter '{DELIMITER}' !!!!!")

def send(record):
    """Buffer records and write them in bulk to a new partitioned file."""
    global batch_records, record_count, file_index, write_header

    batch_records.append(record)
    record_count += 1

    # If partition size is reached, write all buffered records at once
    if record_count >= PARTITION_SIZE:
        flush_partition()  # Write and move to the next file
        file_index += 1  # Increment partition number
        batch_records = []  # Clear batch for the next partition

def flush_partition():
    """Write all collected records in batch to a new compressed CSV file."""
    global batch_records, write_header

    if not batch_records:
        return  # No data to write

    file_name = get_partitioned_filename()

    df = pd.DataFrame(batch_records)

    df.to_csv(
        file_name,
        index=False,
        header=write_header,
        sep=DELIMITER,
        compression="gzip",
        mode="w"  # Overwrite to ensure fresh files
    )

    print(f"!!!!! OUTPUT COMPLETED - File {file_name} written !!!!!")

    # Disable header writing after the first partition
    write_header = False

def close():
    """Flush the remaining records and create auxiliary files."""
    flush_partition()  # Write remaining records before closing
    write_auxiliary_files()  # Create .tok and .txt files

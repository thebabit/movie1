import pandas as pd
import os
import gzip

# Global variables
BASE_FILE_NAME = "output"
PARTITION_SIZE = 1000  # Number of records per partition
file_index = 1
record_count = 0
write_header = True  # Headers only in the first partition
delimiter = ","  # Default delimiter

def get_partitioned_filename():
    """Generate a new gzip CSV filename based on partition index."""
    return f"{BASE_FILE_NAME}_{file_index}.csv.gz"

def start(custom_delimiter=","):
    """Initialize partitioning and set up global variables."""
    global record_count, delimiter, write_header
    delimiter = custom_delimiter
    record_count = 0
    print(f"!!!!! OUTPUT STARTED - Writing with delimiter '{delimiter}' !!!!!")

def send(record):
    """Write a single record to the current partition file."""
    global record_count, file_index, write_header

    file_name = get_partitioned_filename()
    df = pd.DataFrame([record])  # Convert single record to DataFrame

    # Write to the gzip CSV file
    df.to_csv(
        file_name,
        mode="a",  # Append mode to avoid overwriting
        index=False,
        header=write_header,  # Only write header in first partition
        sep=delimiter,
        compression="gzip"
    )

    print(f"Record written to {file_name}: {record}")

    record_count += 1

    # Disable header writing after the first partition
    if write_header:
        write_header = False  

    # If partition size is reached, move to the next partition
    if record_count >= PARTITION_SIZE:
        close()
        file_index += 1  # Move to the next partition
        start(delimiter)  # Reset for next partition

def close():
    """Finalize the current partition (log completion)."""
    print(f"!!!!! OUTPUT COMPLETED - File {get_partitioned_filename()} closed !!!!!")

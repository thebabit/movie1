from pandas import DataFrame


class CSVOutput:
    def __init__(self, app, delivery, output_config):
        self.app = app
        self.delivery = delivery
        self.output_config = output_config
        self.location = self.output_config['testy_location']
        self.rowGroupSize = self.output_config.get('row_group_size', 10)  # Default to writing in batches of 10
        self.records = []
        self.append = False  # Controls whether to append or overwrite

    def _writeRecords(self):
        """Writes the buffered records to a CSV file."""
        df = DataFrame.from_records(self.records)  # Convert records to Pandas DataFrame
        mode = "a" if self.append else "w"  # Append if not the first write
        header = not self.append  # Write header only for the first batch

        df.to_csv(self.location, mode=mode, index=False, header=header)
        self.records = []  # Clear buffer
        self.append = True  # Ensure future writes append instead of overwriting

    def send(self, record):
        """Adds a record to the buffer and writes to CSV when buffer is full."""
        self.records.append(record.getDictRecord())
        if len(self.records) >= self.rowGroupSize:
            self._writeRecords()

    def close(self):
        """Writes remaining records to CSV before closing."""
        if len(self.records) > 0:
            self._writeRecords()









# Configuration for CSV output
output_config = {
    "testy_location": "output.csv",
    "row_group_size": 5  # Write records in batches of 5
}

csv_writer = CSVOutput(app=None, delivery=None, output_config=output_config)

# Simulated Record Class
class Record:
    def __init__(self, data):
        self.data = data

    def getDictRecord(self):
        return self.data

# Send some records
csv_writer.send(Record({"id": 1, "name": "Alice"}))
csv_writer.send(Record({"id": 2, "name": "Bob"}))
csv_writer.send(Record({"id": 3, "name": "Charlie"}))
csv_writer.send(Record({"id": 4, "name": "David"}))
csv_writer.send(Record({"id": 5, "name": "Eve"}))  # This will trigger writing to CSV

csv_writer.send(Record({"id": 6, "name": "Frank"}))
csv_writer.send(Record({"id": 7, "name": "Grace"}))

# Close the writer to flush remaining records
csv_writer.close()








import pandas as pd
import gzip
import os


class CSVOutput:
    def __init__(self, app, delivery, output_config):
        self.app = app
        self.delivery = delivery
        self.output_config = output_config
        self.base_location = self.output_config['testy_location']  # Base filename (without partition suffix)
        self.rowGroupSize = self.output_config.get('row_group_size', 10)  # Batch size before writing
        self.partition_size = self.output_config.get('partition_size', 50)  # Records per partition
        self.records = []
        self.append = False
        self.partition_count = 0  # Keeps track of partition files

    def _get_partition_filename(self):
        """Generates a new partitioned filename with Gzip compression."""
        base_name, ext = os.path.splitext(self.base_location)
        return f"{base_name}_part{self.partition_count}.csv.gz"

    def _writeRecords(self):
        """Writes buffered records to a partitioned and compressed CSV file."""
        if not self.records:
            return  # Nothing to write

        df = pd.DataFrame.from_records(self.records)
        partition_file = self._get_partition_filename()

        # Write to a compressed Gzip file
        with gzip.open(partition_file, "wt", encoding="utf-8") as gz_file:
            df.to_csv(gz_file, index=False, header=not self.append)  # Write headers only for first write

        print(f"Partition {self.partition_count} written: {partition_file}")

        self.records = []  # Clear buffer
        self.append = True  # Ensure subsequent writes do not include headers
        self.partition_count += 1  # Increment partition index

    def send(self, record):
        """Adds a record to the buffer and writes if it reaches partition size."""
        self.records.append(record.getDictRecord())

        if len(self.records) >= self.partition_size:
            self._writeRecords()

    def close(self):
        """Flushes remaining records to a new partition before closing."""
        if self.records:
            self._writeRecords()












# Configuration for CSV output with partitioning
output_config = {
    "testy_location": "output.csv",  # Base filename
    "row_group_size": 10,  # Batch size before writing
    "partition_size": 50  # Max records per partition file
}

csv_writer = CSVOutput(app=None, delivery=None, output_config=output_config)

# Simulated Record Class
class Record:
    def __init__(self, data):
        self.data = data

    def getDictRecord(self):
        return self.data

# Generate test data
for i in range(120):  # Example with 120 records
    csv_writer.send(Record({"id": i + 1, "name": f"Person_{i + 1}"}))

# Close to flush remaining records
csv_writer.close()









class ExternalOutput:
    def __init__(self):
        self.is_running = False
        self.params = {}

    def start(self, **params):
        """Initialize the external output process."""
        self.params = params
        self.is_running = True
        print(f"ExternalOutput started with parameters: {self.params}")

    def send(self, record, headers):
        """Send data to the external output destination."""
        if not self.is_running:
            raise Exception("ExternalOutput has not been started.")
        print(f"Sending record: {record} with headers: {headers}")

    def close(self):
        """Close the external output process."""
        self.is_running = False
        print("ExternalOutput closed.")

# Required function for CustomOutput to load this script
def load_external_script(location):
    """
    Mock function to simulate loading an external script.
    The `location` parameter is just for demonstration.
    """
    print(f"Loading external script from: {location}")
    return ExternalOutput()

def verify_custom_output(instance):
    """
    Mock function to verify that the loaded script has the required methods.
    """
    required_methods = ['start', 'send', 'close']
    return all(hasattr(instance, method) for method in required_methods)





import pandas as pd

# Đọc file CSV
csv_file = "data.csv"
df = pd.read_csv(csv_file)

# Ghi file Parquet
parquet_file = "data.parquet"
df.to_parquet(parquet_file, engine="pyarrow", index=False)

print(f"Đã chuyển đổi {csv_file} sang {parquet_file} thành công!")










import csv
import os
import gzip

# Global variables
BASE_FILE_NAME = "output"
PARTITION_SIZE = 1000  # Number of records per partition
file = None
writer = None
is_started = False
record_count = 0
file_index = 1

def get_partitioned_filename():
    """Generate a new gzip CSV filename based on partition index."""
    return f"{BASE_FILE_NAME}_{file_index}.csv.gz"

def start():
    """Initialize CSV writing by opening a new partitioned gzipped CSV file."""
    global file, writer, is_started, file_index, record_count
    file_name = get_partitioned_filename()
    file = gzip.open(file_name, mode="wt", newline="", encoding="utf-8")  # Open gzip file in text mode
    writer = csv.writer(file)
    record_count = 0  # Reset record counter
    is_started = True
    print(f"!!!!! OUTPUT STARTED - Writing to {file_name} !!!!!")

def send(record, headers):
    """Write a record to the current partitioned and compressed CSV file."""
    global writer, file, is_started, record_count, file_index

    if not is_started:
        raise Exception("CSV output has not been started. Call start() first.")

    # Write headers if it's the first record in the partition
    if record_count == 0 and headers:
        writer.writerow(headers)

    # Write the record
    writer.writerow(record)
    record_count += 1

    # If partition size is reached, close current file and start a new one
    if record_count >= PARTITION_SIZE:
        close()
        file_index += 1  # Move to the next partition
        start()

def close():
    """Close the current partitioned gzipped CSV file."""
    global file, is_started
    if file:
        file.close()
        print(f"!!!!! OUTPUT COMPLETED - File {get_partitioned_filename()} closed !!!!!")
    is_started = False









    import pandas as pd
import os
import gzip

# Global variables
BASE_FILE_NAME = "output"
PARTITION_SIZE = 1000  # Number of records per partition
file_index = 1
record_count = 0
write_header = True  # Headers only in the first partition
buffer = []  # Store records before writing
delimiter = ","  # Default delimiter

def get_partitioned_filename():
    """Generate a new gzip CSV filename based on partition index."""
    return f"{BASE_FILE_NAME}_{file_index}.csv.gz"

def start(custom_delimiter=","):
    """Initialize partitioning and set up global variables."""
    global record_count, buffer, delimiter, write_header
    delimiter = custom_delimiter
    record_count = 0
    buffer = []  # Reset the buffer
    print(f"!!!!! OUTPUT STARTED - Writing with delimiter '{delimiter}' !!!!!")

def send(record):
    """Buffer records and write partitioned files when limit is reached."""
    global buffer, record_count, file_index, write_header

    buffer.append(record)
    record_count += 1

    # If partition size is reached, write to a new file
    if record_count >= PARTITION_SIZE:
        close()
        file_index += 1  # Move to the next partition
        start(delimiter)  # Reset for next partition

def close():
    """Write buffered records to a gzipped CSV file using pandas."""
    global buffer, write_header, record_count

    if buffer:
        file_name = get_partitioned_filename()
        df = pd.DataFrame(buffer)

        # Write headers only in the first partition
        df.to_csv(file_name, index=False, header=write_header, sep=delimiter, compression="gzip")

        print(f"!!!!! OUTPUT COMPLETED - File {file_name} written !!!!!")

        # After first file, disable headers for subsequent partitions
        write_header = False
        buffer.clear()  # Clear the buffer for next partition
        record_count = 0  # Reset record count

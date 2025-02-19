# movie1

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












Yes! Below is the modified version of your ParquetOutput class that writes records to a CSV file instead of a Parquet file. It uses Pandas to handle the CSV writing.

Modified Code: CSVOutput

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


---

Key Changes:

1. Replaced fastparquet.write() with to_csv():

Uses Pandas DataFrame to write to CSV.

Supports batch writing to avoid excessive I/O operations.



2. Handles Append Mode:

The first batch overwrites (w mode) and writes the header.

Subsequent writes append (a mode) without the header.





---

Example Usage:

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


---

Expected Output: output.csv

id,name
1,Alice
2,Bob
3,Charlie
4,David
5,Eve
6,Frank
7,Grace

Data is written in batches of 5.

The first write includes headers, while subsequent writes append data without headers.


Would you like any additional features, such as custom delimiters or error handling?












Yes! You can modify the CSVOutput class to automatically partition the CSV file and compress each partition using Gzip. Below is the enhanced version that splits the CSV into multiple compressed files based on the partition_size (number of records per file).


---

Updated CSVOutput Class with Partitioning & Gzip Compression

import pandas as pd
import gzip
import os


class CSVOutput:
    def __init__(self, app, delivery, output_config):
        self.app = app
        self.delivery = delivery














    Yes! You can modify the CSVOutput class to automatically partition the CSV file and compress each partition using Gzip. Below is the enhanced version that splits the CSV into multiple compressed files based on the partition_size (number of records per file).


---

Updated CSVOutput Class with Partitioning & Gzip Compression

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


---

How It Works:

1. Records are buffered in memory until they reach the partition_size (e.g., 50 records).


2. When full, it writes the records to a new Gzip-compressed CSV file:

Files are named as:

output_part0.csv.gz
output_part1.csv.gz
output_part2.csv.gz



3. Each partition is a separate compressed CSV file, making it easier to manage large datasets.


4. When close() is called, any remaining records are flushed into a final partition.




---

Example Usage:

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


---

Expected Output Files:

1. output_part0.csv.gz (50 records)


2. output_part1.csv.gz (50 records)


3. output_part2.csv.gz (20 records) (Remaining data)



Each file will contain Gzip-compressed CSV data like this (when decompressed):

id,name
1,Person_1
2,Person_2
...
50,Person_50


---

How to Decompress & Read a Partitioned File:

Using Python:

import pandas as pd
df = pd.read_csv("output_part0.csv.gz", compression="gzip")
print(df.head())

Using Command Line:

gunzip -c output_part0.csv.gz | head -n 10


---

Advantages of This Approach:

✔ Automatic partitioning for large datasets
✔ Gzip compression reduces file size
✔ Appends new data into separate files
✔ Efficient memory usage (doesn't load everything at once)

Would you like additional features like custom delimiter support or dynamic naming conventions?



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
    

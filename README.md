import pandas as pd
import os
import gzip
import datetime
import xml.etree.ElementTree as ET
import shutil

# Config
BASE_FILE_NAME = "output"
PARTITION_SIZE = 1000
DELIMITER = "|"
BUCKET = "your-bucket-name"  # Simulated destination
PREFIX = "DMG_DAIFB_MDH_MIDAS_DAILY_MANIFEST_PREFIX"
PATH_PREFIX = "account/dataconditioner/features/metadata"

# State
file_index = 1
record_count = 0
write_header = True
batch_records = []
record_schema = None

def get_partitioned_filename():
    return f"{BASE_FILE_NAME}_{file_index}.csv.gz"

def get_output_file_name(current_time):
    return f"manifest_{current_time.strftime('%Y%m%d_%H%M%S')}.xml"

def get_path(current_time):
    formatted_time = current_time.strftime('%Y%m%d')
    return f"{PATH_PREFIX}/{PREFIX}{formatted_time}-staging"

def get_daily_dmg_xml_root(count):
    upload_file = {
        "fileName": "DoesntMatter",
        "fileSize": "54351163438",
        "recordCount": str(count)
    }

    upload_file_details = {
        "processType": "dailyMDHAccountUploadCuke",
        "uploadId": "092023",
        "performcorrections": "true",
        "removecompleteddataflows": "true",
        "removecompressedfiles": "true",
        "uploadfiles": [upload_file]
    }

    return upload_file_details

def create_xml_string(data):
    root = ET.Element("CreateNewWorkRequest")
    details = ET.SubElement(root, "UploadFileDetails")

    for key in ["processType", "uploadId", "performcorrections", "removecompleteddataflows", "removecompressedfiles"]:
        ET.SubElement(details, key).text = data[key]

    files_element = ET.SubElement(details, "uploadfiles")
    for f in data["uploadfiles"]:
        file_element = ET.SubElement(files_element, "uploadfile")
        for field_key, field_val in f.items():
            ET.SubElement(file_element, field_key).text = field_val

    return ET.tostring(root, encoding="unicode")

def write_manifest_xml():
    now = datetime.datetime.now()
    manifest_data = get_daily_dmg_xml_root(record_count)
    xml_string = create_xml_string(manifest_data).replace("__", "_")

    path = get_path(now)
    os.makedirs(path, exist_ok=True)
    file_name = get_output_file_name(now)
    full_path = os.path.join(path, file_name)

    with open(full_path, "w", encoding="utf-8") as f:
        f.write(xml_string)

    # Simulate copy to final destination
    final_dest = os.path.join(BUCKET, file_name)
    shutil.copy(full_path, final_dest)

    print(f"!!!!! XML MANIFEST CREATED: {final_dest} !!!!!")

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
    write_manifest_xml()

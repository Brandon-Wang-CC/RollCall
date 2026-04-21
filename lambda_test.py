from datetime import datetime, timedelta
import numpy as np
import json
import logging
import boto3
import io
import botocore.exceptions
import os
import pandas as pd

# =========================
# Logging
# =========================
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# =========================
# AWS Clients
# =========================
s3 = boto3.client("s3", region_name="us-east-1")

# =========================
# Constants
# =========================
BUCKET_NAME = "rollcall-s3-dev-67"
DEPTS_BUCKET = "rollcall-s3-dev-depts-reference"
DEPTS_FILE = "cc_id.csv"
TMP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tmp")

FILE_PREFIXES = {
    "unfilled":   "ES&F_GR&S Unfilled Requisition Report",
    "contractor": "NEW-IT Contractor-VG-Vendor Req Report-",
    "candidates": "GR&S Candidate Flow Weekly Report",
}

FILTER_CONFIG = {
    "unfilled": {
        "sheet":          "Sheet1",
        "anchor_columns": ["Subdivision", "Requisition Number"],
        "filter_column":  "Subdivision",
        "filter_value":   "Chief Information Security Office",
    },
    "contractor_open": {
        "sheet":          "Open",
        "anchor_columns": ["Sub-Division", "Req #"],
        "filter_column":  "Sub-Division",
        "filter_value":   "ES&F",
    },
    "contractor_closed": {
        "sheet":          "Closed",
        "anchor_columns": ["Sub-Division", "Req #"],
        "filter_column":  "Sub-Division",
        "filter_value":   "ES&F",
    },
}

# =========================
# File Discovery
# =========================
def get_newest_file_by_prefix(bucket_name, prefix):
    response = s3.list_objects_v2(Bucket=bucket_name)
    matches = [
        obj for obj in response.get("Contents", [])
        if obj["Key"].startswith(prefix) and obj["Key"].endswith(".xlsx")
    ]
    if not matches:
        raise FileNotFoundError(f"No files found with prefix: {prefix}")
    newest = max(matches, key=lambda x: x["LastModified"])
    logger.info(f"Found newest file for '{prefix}': {newest['Key']}")
    return newest["Key"]

def discover_files(bucket_name):
    logger.info("Starting file discovery...")
    files = {}
    for name, prefix in FILE_PREFIXES.items():
        files[name] = get_newest_file_by_prefix(bucket_name, prefix)
    logger.info(f"All files discovered: {files}")
    return files

# =========================
# File Downloading
# =========================
def download_file(bucket_name, key):
    filename = key.split("/")[-1]
    local_path = f"{TMP_DIR}/{filename}"
    logger.info(f"Downloading '{key}' to '{local_path}'...")
    s3.download_file(bucket_name, key, local_path)
    logger.info(f"Downloaded successfully: {local_path}")
    return local_path

def download_all_files(bucket_name, discovered_files):
    os.makedirs(TMP_DIR, exist_ok=True)
    logger.info("Starting file downloads...")
    local_files = {}
    for name, key in discovered_files.items():
        local_files[name] = download_file(bucket_name, key)
    logger.info(f"All files downloaded: {local_files}")
    return local_files

# =========================
# Header Detection
# =========================
def find_header_row(local_path, known_columns, sheet_name=None):
    logger.info(f"Searching for header row in '{local_path}'...")
    df = pd.read_excel(local_path, sheet_name=sheet_name, header=None)
    for i, row in df.iterrows():
        if all(col in row.values for col in known_columns):
            logger.info(f"Header row found at row {i + 1}")
            return i + 1
    raise ValueError(f"Could not find header row containing all of: {known_columns}")

# =========================
# Filtering
# =========================
def load_and_filter(local_path, sheet_name, anchor_columns, filter_column, filter_value):
    logger.info(f"Loading '{local_path}' sheet '{sheet_name}'...")
    header_row = find_header_row(local_path, anchor_columns, sheet_name)
    df = pd.read_excel(local_path, sheet_name=sheet_name, header=header_row - 1)
    logger.info(f"Loaded {len(df)} rows, applying filter '{filter_column}' == '{filter_value}'...")
    filtered = df[df[filter_column] == filter_value]
    logger.info(f"Filter complete, {len(filtered)} rows remaining")
    return filtered

def filter_all_files(local_files):
    logger.info("Starting filtering...")
    results = {}
    for name, config in FILTER_CONFIG.items():
        source_key = "contractor" if "contractor" in name else name
        results[name] = load_and_filter(
            local_files[source_key],
            config["sheet"],
            config["anchor_columns"],
            config["filter_column"],
            config["filter_value"],
        )
    logger.info("All filtering complete!")
    return results

# =========================
# Depts Reference
# =========================
def load_dept_codes(bucket_name, key):
    logger.info("Loading department codes from S3...")
    response = s3.get_object(Bucket=bucket_name, Key=key)
    df = pd.read_csv(io.StringIO(response["Body"].read().decode("utf-8")))
    codes = set(pd.to_numeric(df["cc_id"], errors="coerce").dropna().astype(int))
    logger.info(f"Loaded {len(codes)} department codes")
    return codes

def filter_candidates(local_path, dept_codes):
    logger.info("Filtering candidates file...")
    header_row = find_header_row(local_path, ["Candidate Name", "Candidate Status"], sheet_name="Sheet1")
    df = pd.read_excel(local_path, sheet_name="Sheet1", header=header_row - 1)

    # Match cost center numerically, same as macro's LEFT(...,4)*1
    df["cc_match"] = pd.to_numeric(
        df["Cost Center"].astype(str).str.strip().str[:4], errors="coerce"
    )
    filtered = df[df["cc_match"].isin(dept_codes)].drop(columns=["cc_match"])

    # Drop completely empty rows (equivalent to what old macro's RemoveDuplicates was doing)
    filtered = filtered.dropna(how="all")

    logger.info(f"Candidates filter complete, {len(filtered)} rows remaining")
    return filtered

# =========================
# Reference Data
# =========================
ESF_WF_FILE = "ESF WF data file.xlsx"
CC_ID_FILE = "cc_id.csv"
DEPTS_FILE = "depts.csv"
STATUS_FILE = "status.csv"

def load_reference_data(bucket_name):
    logger.info("Loading reference data from S3...")

    # --- cc_id: cc_id -> subdepartment ---
    response = s3.get_object(Bucket=bucket_name, Key=CC_ID_FILE)
    cc_id_df = pd.read_csv(io.StringIO(response["Body"].read().decode("utf-8")))
    cc_id_df["cc_id"] = pd.to_numeric(cc_id_df["cc_id"], errors="coerce")
    logger.info(f"Loaded cc_id: {len(cc_id_df)} rows")

    # --- depts: department -> MD-2 ---
    response = s3.get_object(Bucket=bucket_name, Key=DEPTS_FILE)
    depts_df = pd.read_csv(io.StringIO(response["Body"].read().decode("utf-8")))
    logger.info(f"Loaded depts: {len(depts_df)} rows")

    # --- status: full status -> short status ---
    response = s3.get_object(Bucket=bucket_name, Key=STATUS_FILE)
    status_df = pd.read_csv(io.StringIO(response["Body"].read().decode("utf-8")))
    logger.info(f"Loaded status: {len(status_df)} rows")

    # --- ESF WF data file: Reqs and ALL sheets ---
    response = s3.get_object(Bucket=bucket_name, Key=ESF_WF_FILE)
    esf_bytes = io.BytesIO(response["Body"].read())

    esf_reqs_df = pd.read_excel(esf_bytes, sheet_name="Reqs")
    esf_reqs_df = esf_reqs_df.loc[:, ~esf_reqs_df.columns.str.startswith("Unnamed")]
    esf_reqs_df["Req #"] = pd.to_numeric(esf_reqs_df["Req #"], errors="coerce")
    logger.info(f"Loaded ESF WF Reqs: {len(esf_reqs_df)} rows")

    esf_bytes.seek(0)  # Reset the byte stream before reading again
    esf_all_df = pd.read_excel(esf_bytes, sheet_name="ALL")
    logger.info(f"Loaded ESF WF ALL: {len(esf_all_df)} rows")

    logger.info("All reference data loaded!")
    return {
        "cc_id":    cc_id_df,
        "depts":    depts_df,
        "status":   status_df,
        "esf_reqs": esf_reqs_df,
        "esf_all":  esf_all_df,
    }

# =========================
# Three days pre monday
# =========================
def get_three_days_pre_monday():
    """
    Calculate the date that is 3 days before the last Monday. For Contractor Filled logic.
    """
    today = datetime.now()
    # Calculate days since last Monday (0=Monday, 6=Sunday)
    days_since_monday = (today.weekday() - 0) % 7
    last_monday = today - timedelta(days=days_since_monday)
    three_days_pre_monday = last_monday - timedelta(days=3)
    return three_days_pre_monday

# =========================
# Build Green Sheets
# =========================    

def build_crew_unfilled(filtered, ref):
    # returns a DataFrame
    pass

def build_crew_filled(filtered, ref):
    # returns a DataFrame
    pass

def build_contractor_unfilled(filtered, ref):
    # returns a DataFrame
    pass

def build_contractor_filled(filtered, ref):
    # returns a DataFrame
    cc = filtered["contractor_closed"].copy()
    esf_all = ref["esf_all"].copy()
    df = pd.DataFrame(columns=['Status', 'Department', 'WorkerType', 'JobProfile', 'CostCenter', 'GradeLevel', 'Management', 'ManagerName', 'MD1', 'MD2', 'ReqNumber', 'HireName', 'StartDate', 'State'])

    # ~Department Check
    cc_df = ref["cc_id"]
    try:
        df["Department"] = pd.merge(df, cc_df, left_on="CostCenter", right_on='cc_id', how="inner")
    except pd.errors.MergeError:
        df["Department"] = df["Department"].fillna("")

    # ~Infosys override
    infosys_check = df["Department"] == "Infosys"

    if infosys_check:
        #Need assistance
        pass

    # ~Req Number Check

    # ~Cost Center Check

    # ~Manager Name Check

    # ~Job Profile Check

    # ~MD2 Check

    # ~Hire Name Check
    # ~Start Date Check
    # ~State Check

    # ~Grade Level Check
    df["GradeLevel"] = np.where(df["ReqNumber"].notna(), "00", "")

    # ~Management Check
    df["Management"] = np.where(df["ReqNumber"].notna(), "Non-Management", "")
    # ~MD1 Check
    df["Management"] = np.where(df["ReqNumber"].notna(), "Manish Nagar (019067)", "")
    # ~Worker Type Check
    df["WorkerType"] = np.where(df["CostCenter"].notna(), "Contractor", "")  # Test Case: What if this is something random that isn't blank or contractor?

    # ~Status Check
    date_for_contractor = get_three_days_pre_monday()

    #If ReqNumber is empty. Then set it to be empy.
    # df["Status"] = np.where(df["ReqNumber"] == "", "", np.where(df.merge(esf_all[])))

    # ISERROR(VLOOKUP()) Look up current ReqNumber in ESF file and grab the department value and see if its valid



    return df




def write_output_workbook(crew_unfilled, crew_filled, contractor_unfilled, contractor_filled):
    # writes single Excel workbook to /tmp and uploads to S3
    pass

# =========================
# Main / Test
# =========================
if __name__ == "__main__":
    discovered = discover_files(BUCKET_NAME)
    local_files = download_all_files(BUCKET_NAME, discovered)
    filtered = filter_all_files(local_files)

    dept_codes = load_dept_codes(DEPTS_BUCKET, CC_ID_FILE)
    filtered["candidates"] = filter_candidates(local_files["candidates"], dept_codes)

    for name, df in filtered.items():
        print(f"{name}: {len(df)} rows")

    ref = load_reference_data(DEPTS_BUCKET)
    for name, df in ref.items():
        print(f"{name}: {len(df)} rows, columns: {list(df.columns)}")
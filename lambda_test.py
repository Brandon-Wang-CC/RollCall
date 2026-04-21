import json
import logging
import boto3
import io
import botocore.exceptions
import os
import pandas as pd
import numpy as np


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
# Build Green Sheets
# =========================    

def build_crew_unfilled(filtered, ref):
    # returns a DataFrame
    pass

def build_crew_filled(filtered, ref):

    candidates = filtered["candidates"].copy()
    esf_reqs = ref["esf_reqs"].copy()
    depts = ref["depts"].copy()
    status_map = ref["status"].copy()

    # =========================
    # Normalize keys
    # =========================
    candidates["Req #"] = pd.to_numeric(candidates["Req #"], errors="coerce")
    esf_reqs["Req #"] = pd.to_numeric(esf_reqs["Req #"], errors="coerce")

    req_check = set(esf_reqs["Req #"].dropna())

    # =========================
    # FILTER (exact Excel logic)
    # =========================
    valid_status = [
        "Offer",
        "Employment Agreement",
        "Ready for Hire",
        "Background Check",
    ]

    # ==========================================================
    # !!! Instructions!B3 DATE LOGIC — FINAL REVIEW POINT !!!
    # ==========================================================
    control_date = pd.Timestamp.today().normalize()  # <-- REPLACE if needed
    cutoff = control_date - pd.Timedelta(days=6)
    # ==========================================================

    start_dates = pd.to_datetime(candidates["Start Date"], errors="coerce")

    mask = (
            candidates["Req #"].isin(req_check)
            & candidates["Candidate Status"].isin(valid_status)
            & (
                    (start_dates >= cutoff)
                    | (start_dates.isna())
            )
    )

    df = candidates.loc[mask].copy()

    # =========================
    # Cost Center (LEFT 4)
    # =========================
    df["Cost Center"] = (
        df["Cost Center"].astype(str).str.strip().str[:4]
    )
    df["cc_id"] = pd.to_numeric(df["Cost Center"], errors="coerce")

    # =========================
    # Department (Depts A:B)
    # =========================
    depts_ab = depts.iloc[:, [0, 1]].copy()
    depts_ab.columns = ["cc_id", "Department"]
    df = df.merge(depts_ab, on="cc_id", how="left")

    # =========================
    # MD-2 (Depts E:F)
    # =========================
    depts_ef = depts.iloc[:, [4, 5]].copy()
    depts_ef.columns = ["Department", "MD-2"]
    df = df.merge(depts_ef, on="Department", how="left")

    # =========================
    # Status mapping
    # =========================
    status_map.columns = ["Full Status", "Short Status"]
    df = df.merge(
        status_map,
        left_on="Candidate Status",
        right_on="Full Status",
        how="left"
    )

    # =========================
    # Merge ESF REQS
    # =========================
    df = df.merge(
        esf_reqs,
        on="Req #",
        how="left",
        suffixes=("", "_req")
    )

    # =========================
    # Existing vs New (EXACT)
    # =========================
    lookup_name = df["Hire Name_req"]
    lookup_date = pd.to_datetime(df["Start Date_req"], errors="coerce")

    hire_name = df["Candidate Name"]
    start_date = pd.to_datetime(df["Start Date"], errors="coerce")

    df["Existing v New"] = np.where(
        df["Req #"].isna() | (df["Req #"] == 0),
        "",
        np.where(
            lookup_name.astype(str) != hire_name.astype(str),
            "Update",
            np.where(
                lookup_date != start_date,
                "Update Date",
                "Existing"
            )
        )
    )

    # =========================
    # Management Type
    # =========================
    df["Management Type"] = np.where(
        df.get("Job Level", "").astype(str).str.contains("M", na=False),
        "Management",
        "Non Management"
    )

    # =========================
    # Static fields
    # =========================
    df["Worker Type"] = "Regular"
    df["MD-1"] = "Manish Nagar (019067)"
    df["Location"] = "Crew"

    # =========================
    # Final Output
    # =========================
    output = pd.DataFrame({
        "Existing v New": df["Existing v New"],
        "Department": df["Department"],
        "Worker Type": df["Worker Type"],
        "Job Code": df["Job Code_req"],
        "Job Profile": df["Job Profile_req"],
        "Cost Center": df["Cost Center"],
        "Grade level": df.get("Job Level"),
        "Management Type": df["Management Type"],
        "Manager Name": df.get("Manager"),
        "MD-1": df["MD-1"],
        "MD-2": df["MD-2"],
        "Status": df["Short Status"],
        "Req #": df["Req #"],
        "FTE": np.nan,
        "Location": df["Location"],
        "Hire Name": hire_name,
        "Start Date": start_date,
        "State": df.get("State"),
        "Job Requisition Primary Location (Building)": df.get("Primary Location"),
        "Job Requisition Additional Locations": np.nan,
        "Comment": df["Comment_req"],
    })

    return output

def build_contractor_unfilled(filtered, ref):
    # returns a DataFrame
    pass

def build_contractor_filled(filtered, ref):
    # returns a DataFrame
    pass

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
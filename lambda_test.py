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
# Three days pre monday for Status
# =========================
def get_three_days_pre_monday():
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
    """
       Converts the Excel "Contractor Filled" tab formulas into pandas logic.

       All column-letter references below map to the Excel formula spec:
         Col A = Status,  Col B = Department,  Col C = Worker Type,
         Col E = Job Profile,  Col F = Cost Center,  Col G = Grade Level,
         Col H = Management,  Col I = Manager Name,  Col J = MD-1,
         Col K = MD-2,  Col L = Status (Short),  Col M = Req #,
         Col N = FTE,  Col Q = Hire Name,  Col R = Start Date,
         Col S = State,  Col W = Contractor Req Status
       """
    #Clones of the dataframes
    cc = filtered["contractor_closed"].copy()
    esf_reqs = ref["esf_reqs"].copy()
    df = pd.DataFrame(columns=['Status', 'Department', 'WorkerType', 'JobProfile', 'CostCenter', 'GradeLevel', 'Management', 'ManagerName', 'MD1', 'MD2', 'ReqNumber', 'HireName', 'StartDate', 'State', 'ContractorReqStatus', 'SecondStatus'])

    # ------------------------------------------------------------------
    # REQ NUMBER
    # ------------------------------------------------------------------
    df["ReqNumber"] = pd.to_numeric(cc["Req #"], errors="coerce")
    # ------------------------------------------------------------------
    # COST CENTER
    # ------------------------------------------------------------------
    df["CostCenter"] = cc["Cost Center"].values
    # ------------------------------------------------------------------
    # Manager Name
    # ------------------------------------------------------------------
    df["ManagerName"] = cc["Hiring Manager"].values
    # ------------------------------------------------------------------
    # Job Profile
    # ------------------------------------------------------------------
    df["JobProfile"] = cc["Job Tile (Standardized)"].values
    # ------------------------------------------------------------------
    # Start Date
    # ------------------------------------------------------------------
    df["StartDate"] = cc["Start Date"].values
    # ------------------------------------------------------------------
    # State Check
    # ------------------------------------------------------------------
    df["State"] = cc["LOC"].values
    # ------------------------------------------------------------------
    # Contractor REQ Status Check?
    # ------------------------------------------------------------------
    df["ContractorReqStatus"] = cc['Status\n(Please Make a Selection from List)'].values

    # ------------------------------------------------------------------
    # Department
    # ------------------------------------------------------------------
    cc_id_df = ref["cc_id"].copy()
    cc_id_df["cc_id"] = pd.to_numeric(cc_id_df["cc_id"], errors="coerce")
    cc_to_subdept = cc_id_df.dropna(subset=["cc_id"]).set_index("cc_id")["subdepartment"].to_dict()

        # --- ESF ALL: for Infosys override ---
        # Formula: INDEX(ALL!$A:$R, MATCH(ManagerName, ALL!$R:$R, 0), 1)
        # Col R in ALL (0-indexed col 17) = "Cost Center ##" in sample file.
        # Matches Manager Name against that column and returns col A (Department).
    esf_all = ref["esf_all"]
    all_col_r_name = esf_all.columns[17]
    infosys_lookup = (
        esf_all.dropna(subset=[all_col_r_name])
        .set_index(all_col_r_name)["Department"]
        .to_dict()
    )
    def lookup_department(cost_center, manager_name) -> str:
        try:
            cc_key = int(str(cost_center).strip()[:4])
            subdept = cc_to_subdept.get(cc_key)
            if subdept is None:
                return ""
            if subdept == "Infosys":
                return infosys_lookup.get(manager_name, "")
            return subdept
        except (ValueError, TypeError):
            return ""

    df["Department"] = [
        lookup_department(cc, mgr)
        for cc, mgr in zip(df["CostCenter"], df["ManagerName"])
    ]

    # ------------------------------------------------------------------
    # Grade Level
    # ------------------------------------------------------------------
    df["GradeLevel"] = np.where(df["ReqNumber"].notna(), "00", "")
    # ------------------------------------------------------------------
    # Management
    # ------------------------------------------------------------------
    df["Management"] = np.where(df["ReqNumber"].notna(), "Non-Management", "")
    # ------------------------------------------------------------------
    # MD1
    # ------------------------------------------------------------------
    df["MD1"] = np.where(df["ReqNumber"].notna(), "Manish Nagar (019067)", "")
    # ------------------------------------------------------------------
    # Worker Type
    # ------------------------------------------------------------------
    df["WorkerType"] = np.where(df["CostCenter"].notna(), "Contractor", "")  # Test Case: What if this is something random that isn't blank or contractor?
    # ------------------------------------------------------------------
    # Hire Name
    # ------------------------------------------------------------------
    esf_reqs["Req #"] = pd.to_numeric(esf_reqs["Req #"], errors="coerce") #Convert the strings to numbers
    req_to_hire_name = (
        esf_reqs.dropna(subset=["Req #"])
                .set_index("Req #")["Hire Name"]
                .to_dict()
        if "Hire Name" in esf_reqs.columns else {}
    )
    df["HireName"] = [
        "" if pd.isna(req) else req_to_hire_name.get(req, "")
        for req in df["ReqNumber"]
    ]
    # ------------------------------------------------------------------
    # Status Check
    # ------------------------------------------------------------------
    req_exists_set = set(esf_reqs["Req #"].dropna())
    date_for_contractor = get_three_days_pre_monday()

    def compute_status(req_num, start_date, hire_name: str) -> str:
        if pd.isna(req_num):
            return ""
        in_reqs = req_num in req_exists_set  # ISERROR -> not in set
        if not in_reqs:
            start = pd.to_datetime(start_date, errors="coerce")
            if pd.notna(start) and start < date_for_contractor:
                return "Validate if started"
            return "NEW"
        # Req exists: check Hire Name (col P / 5th from L)
        return "Newly Filled" if not hire_name else "Filled"

    df["Status"] = [
        compute_status(req, sd, hn)
        for req, sd, hn in zip(df["ReqNumber"], df["StartDate"], df["HireName"])
    ]
    # ------------------------------------------------------------------
    # Second Status?
    # ------------------------------------------------------------------
    depts_df = ref["depts"]

    # Status short (Col L): IFERROR(VLOOKUP(W5, Depts!K:L, 2, 0), "")
    # Depts col K = "Status" (full text), col L = "Unnamed: 11" (short label)
    status_map = (
        depts_df.dropna(subset=["Status"])
        .set_index("Status")["Unnamed: 11"]
        .to_dict()
    )
    df["SecondStatus"] = [status_map.get(str(s), "") for s in df["ContractorReqStatus"]]

    # ------------------------------------------------------------------
    # MD2
    # ------------------------------------------------------------------
    dept_to_md2 = (
        depts_df.dropna(subset=["Department"])
        .set_index("Department")["Unnamed: 7"]  # 4th col of E:J range
        .to_dict()
    )
    req_to_dept_head = (
        df.dropna(subset=["Req #"])
        .set_index("Req #")["Dept Head"]
        .to_dict()
        if "Dept Head" in df.columns else {}
    )

    def compute_md2(status_a: str, dept: str, req_num) -> str:
        if not status_a:
            return ""
        md2 = dept_to_md2.get(dept)
        if md2 is None:  # ISERROR -> fallback to Dept Head from ContractorClosed
            return str(req_to_dept_head.get(req_num, ""))
        return str(md2)

    df["MD2"] = [
        compute_md2(st, dept, req)
        for st, dept, req in zip(df["Status"], df["Department"], df["ReqNumber"])
    ]

    #Final Return
    return df



def write_output_workbook(crew_unfilled, crew_filled, contractor_unfilled, contractor_filled):
    # writes single Excel workbook to /tmp and uploads to S3
    pass

# =========================
# Main / Test
# =========================
if __name__ == "__main__":
    # discovered = discover_files(BUCKET_NAME)
    # local_files = download_all_files(BUCKET_NAME, discovered)
    # filtered = filter_all_files(local_files)

    # dept_codes = load_dept_codes(DEPTS_BUCKET, CC_ID_FILE)
    # filtered["candidates"] = filter_candidates(local_files["candidates"], dept_codes)

    # for name, df in filtered.items():
    #     print(f"{name}: {len(df)} rows")

    # ref = load_reference_data(DEPTS_BUCKET)
    # for name, df in ref.items():
    #     print(f"{name}: {len(df)} rows, columns: {list(df.columns)}")
    discovered = discover_files(BUCKET_NAME)
    local_files = download_all_files(BUCKET_NAME, discovered)
    filtered = filter_all_files(local_files)

    dept_codes = load_dept_codes(DEPTS_BUCKET, CC_ID_FILE)
    filtered["candidates"] = filter_candidates(local_files["candidates"], dept_codes)

    ref = load_reference_data(DEPTS_BUCKET)

    # --- Inspect reference column names before running build ---
    print("depts columns:", list(ref["depts"].columns))
    print("esf_all columns:", list(ref["esf_all"].columns))
    print("esf_reqs columns:", list(ref["esf_reqs"].columns))
    print("contractor_closed columns:", list(filtered["contractor_closed"].columns))

    result = build_contractor_filled(filtered, ref)
    print(result.head())
    print(result.dtypes)
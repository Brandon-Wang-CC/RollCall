import json
import logging
import time
import boto3
import io
import botocore.exceptions
import os
import pandas as pd
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3   = boto3.client("s3")
ses  = boto3.client("ses")

BUCKET_NAME  = os.environ.get("CSV_BUCKET")
DEPTS_BUCKET = os.environ.get("DEPTS_BUCKET")
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
TMP_DIR = "/tmp" if os.environ.get("AWS_EXECUTION_ENV") else os.path.join(os.path.dirname(os.path.abspath(__file__)), "tmp")


def _decode(val):
    return (val or "").replace("*", " ")

FILE_PREFIXES = {
    "unfilled":          _decode(os.environ.get("FILE_PREFIX_UNFILLED")),
    "contractor_open":   _decode(os.environ.get("FILE_PREFIX_CONTRACTOR_OPEN")),
    "contractor_closed": _decode(os.environ.get("FILE_PREFIX_CONTRACTOR_CLOSED")),
    "candidates":        _decode(os.environ.get("FILE_PREFIX_CANDIDATES")),
}

MD1_NAME = _decode(os.environ.get("MD1_NAME"))

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

def get_newest_file_by_prefix(bucket_name, prefix):
    response = s3.list_objects_v2(Bucket=bucket_name)
    all_objects = response.get("Contents", [])
    logger.info("ListObjectsV2: %d object(s) in bucket, searching for prefix '%s'", len(all_objects), prefix)
    matches = [
        obj for obj in all_objects
        if obj["Key"].startswith(prefix) and obj["Key"].endswith(".xlsx")
    ]
    if not matches:
        raise FileNotFoundError(f"No files found with prefix: {prefix}")
    newest = max(matches, key=lambda x: x["LastModified"])
    logger.info(f"Found newest file for '{prefix}': {newest['Key']} (last modified: {newest['LastModified']})")
    return newest["Key"]

def discover_files(bucket_name):
    logger.info("Starting file discovery...")
    files = {}
    for name, prefix in FILE_PREFIXES.items():
        files[name] = get_newest_file_by_prefix(bucket_name, prefix)
    logger.info(f"All files discovered: {files}")
    return files

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

def find_header_row(local_path, known_columns, sheet_name=None):
    logger.info(f"Searching for header row in '{local_path}'...")
    df = pd.read_excel(local_path, sheet_name=sheet_name, header=None)
    for i, row in df.iterrows():
        if all(col in row.values for col in known_columns):
            logger.info(f"Header row found at row {i + 1}")
            return i + 1
    raise ValueError(f"Could not find header row containing all of: {known_columns}")

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
        results[name] = load_and_filter(
            local_files[name],
            config["sheet"],
            config["anchor_columns"],
            config["filter_column"],
            config["filter_value"],
        )
    logger.info("All filtering complete!")
    return results

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

ESF_WF_FILE = "ESF WF data file ref.xlsx"
CC_ID_FILE  = "cc_id.csv"
DEPTS_FILE  = "depts.csv"
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

    esf_bytes.seek(0)  # must rewind before the second read
    esf_all_df = pd.read_excel(esf_bytes, sheet_name="ALL")
    logger.info(f"Loaded ESF WF ALL: {len(esf_all_df)} rows")

    # Req numbers seen in any previous run — used so reqs marked NEW in run N
    # are classified Existing/Open in run N+1 rather than staying NEW forever.
    prev_req_nums = set()
    try:
        prev_response = s3.get_object(Bucket=bucket_name, Key=REF_KEY)
        prev_bytes = io.BytesIO(prev_response["Body"].read())
        prev_xl = pd.ExcelFile(prev_bytes)
        sheet = "Output" if "Output" in prev_xl.sheet_names else (
                "Reqs"   if "Reqs"   in prev_xl.sheet_names else None)
        if sheet:
            prev_df = pd.read_excel(prev_xl, sheet_name=sheet, usecols=["Req #"])
            prev_req_nums = {_normalize_req(v) for v in prev_df["Req #"] if _normalize_req(v)}
            logger.info("Loaded %d req numbers from previous run", len(prev_req_nums))
    except Exception:
        logger.info("No previous run reference found — all reqs evaluated against static seed only")

    logger.info("All reference data loaded!")
    return {
        "cc_id":          cc_id_df,
        "depts":          depts_df,
        "status":         status_df,
        "esf_reqs":       esf_reqs_df,
        "esf_all":        esf_all_df,
        "prev_req_nums":  prev_req_nums,
    }
    
def build_crew_unfilled(filtered, ref):
    logger.info("Building Crew Unfilled...")
    df            = filtered["unfilled"].copy()
    candidates    = filtered["candidates"].copy()
    esf_reqs      = ref["esf_reqs"].copy()
    cc_id         = ref["cc_id"].copy()
    prev_req_nums = ref.get("prev_req_nums", set())
    depts      = ref["depts"].copy()

    esf_reqs["Req #"] = pd.to_numeric(esf_reqs["Req #"], errors="coerce")
    req_check = set(esf_reqs["Req #"].dropna().astype(int))

    # Req number is the first 7 chars of the "Job Requisition" field
    candidates["req_int"] = pd.to_numeric(
        candidates["Job Requisition"].astype(str).str.strip().str[:7],
        errors="coerce"
    )

    rows = []
    for _, row in df.iterrows():
        req_num = row.get("Requisition Number")
        if pd.isna(req_num):
            continue
        req_num_int = int(req_num)

        # Cost Center -> Department (subdepartment) via cc_id
        cost_center = row.get("Cost Center ID", "")
        cost_center_num = pd.to_numeric(cost_center, errors="coerce")
        dept_match = cc_id[cc_id["cc_id"] == cost_center_num]
        department = dept_match.iloc[0]["subdepartment"] if not dept_match.empty else ""

        # Department -> MD-2 via depts
        md2_match = depts[depts["department"] == department]
        md2 = md2_match.iloc[0]["MD-2"] if not md2_match.empty else ""

        existing_v_new = "Existing" if (req_num_int in req_check or str(req_num_int) in prev_req_nums) else "NEW"

        grade_level = row.get("Grade Grouping - GTA", "")
        grade_str   = str(grade_level) if pd.notna(grade_level) else ""
        management_type = "Management" if "M" in grade_str else "Non Management"

        hire_match = candidates[
            (candidates["req_int"] == req_num_int) &
            (candidates["Candidate Status"] == "Ready for Hire")
        ]
        hire_name = hire_match.iloc[0]["Candidate Name"] if not hire_match.empty else ""

        rows.append({
            "Existing v New":                              existing_v_new,
            "Department":                                  department,
            "Worker Type":                                 "Regular",
            "Job Code":                                    row.get("ID", ""),
            "Job Profile":                                 row.get("Job Profile Name", ""),
            "Cost Center":                                 cost_center,
            "Grade level":                                 grade_level,
            "Management Type":                             management_type,
            "Manager Name":                                row.get("Hiring Manager Name", ""),
            "MD-1":                                        MD1_NAME,
            "MD-2":                                        md2,
            "Status":                                      row.get("Job Requisition Status", ""),
            "Req #":                                       req_num_int,
            "FTE":                                         row.get("Number of Openings Total", ""),
            "Location":                                    "",
            "Note":                                        "",
            "Hire Name":                                   hire_name,
            "Start Date":                                  "",
            "State":                                       row.get("State", ""),
            "Job Requisition Primary Location (Building)": row.get("Job Requisition Primary Location (Building)", ""),
            "Job Requisition Additional Locations":        row.get("Job Requisition Additional Locations", ""),
            "Comment":                                     "",
        })

    result = pd.DataFrame(rows)
    logger.info(f"Crew Unfilled complete: {len(result)} rows")
    return result


def build_crew_filled(filtered, ref):
    logger.info("Building Crew Filled...")
    candidates = filtered["candidates"].copy()
    esf_reqs   = ref["esf_reqs"].copy()
    cc_id      = ref["cc_id"].copy()
    depts      = ref["depts"].copy()
    status_df  = ref["status"].copy()

    # ESF Reqs req numbers as integers — no duplicates confirmed
    esf_reqs["Req #"] = pd.to_numeric(esf_reqs["Req #"], errors="coerce")
    req_check = set(esf_reqs["Req #"].dropna().astype(int))
    esf_reqs_indexed = esf_reqs.dropna(subset=["Req #"]).set_index("Req #")

    # Status mapping: Candidate Status -> short status
    status_map = dict(zip(status_df["status"], status_df["short status"]))

    # Cutoff date: Instructions!$B$3 - 6 days = today - 6
    cutoff_date = pd.Timestamp.today() - pd.Timedelta(days=6)

    # Extract numeric req number from first 7 chars of Job Requisition
    candidates["req_int"] = pd.to_numeric(
        candidates["Job Requisition"].astype(str).str.strip().str[:7],
        errors="coerce"
    )

    # Filter candidates:
    # 1. req_int in req_check (Req Check list = ESF Reqs req numbers)
    # 2. Candidate Status in target statuses
    # 3. Candidate Start Date >= cutoff OR Start Date is 0/NaN
    start_dates = pd.to_datetime(candidates["Candidate Start Date"], errors="coerce")
    df = candidates[
        candidates["req_int"].isin(req_check) &
        candidates["Candidate Status"].isin([
            "Offer", "Employment Agreement", "Ready for Hire", "Background Check"
        ]) &
        ((start_dates >= cutoff_date) | (candidates["Candidate Start Date"] == 0) | start_dates.isna())
    ].copy()

    rows = []
    for _, row in df.iterrows():
        req_num = row.get("req_int")
        if pd.isna(req_num):
            continue
        req_num_int = int(req_num)

        # Cost Center: LEFT 4 of Cost Center column
        cost_center_4 = str(row.get("Cost Center", "")).strip()[:4]
        cost_center_num = pd.to_numeric(cost_center_4, errors="coerce")

        # Cost Center -> Department
        dept_match = cc_id[cc_id["cc_id"] == cost_center_num]
        department = dept_match.iloc[0]["subdepartment"] if not dept_match.empty else ""

        # Department -> MD-2
        md2_match = depts[depts["department"] == department]
        md2 = md2_match.iloc[0]["MD-2"] if not md2_match.empty else ""

        hire_name  = row.get("Candidate Name", "")
        start_date = row.get("Candidate Start Date", "")

        if req_num_int in esf_reqs_indexed.index:
            esf_row     = esf_reqs_indexed.loc[req_num_int]
            job_code    = esf_row.get("Job Code", "")
            job_profile = esf_row.get("Job Profile", "")
            comment     = esf_row.get("Comment", "")
            esf_hire    = esf_row.get("Hire Name", "")
            esf_start   = esf_row.get("Start Date", "")

            # "Update" if hire name or start date changed since the last ESF snapshot
            if str(esf_hire) != str(hire_name):
                existing_v_new = "Update"
            elif str(esf_start) != str(start_date):
                existing_v_new = "Update Date"
            else:
                existing_v_new = "Existing"
        else:
            job_code       = ""
            job_profile    = ""
            comment        = ""
            existing_v_new = "Existing"

        short_status = status_map.get(row.get("Candidate Status", ""), "")

        grade_level = row.get("Grade", "")
        grade_str   = str(grade_level) if pd.notna(grade_level) else ""
        management_type = "Management" if "M" in grade_str else "Non Management"

        rows.append({
            "Existing v New":                              existing_v_new,
            "Department":                                  department,
            "Worker Type":                                 "Regular",
            "Job Code":                                    job_code,
            "Job Profile":                                 job_profile,
            "Cost Center":                                 cost_center_4,
            "Grade level":                                 grade_level,
            "Management Type":                             management_type,
            "Manager Name":                                row.get("Hiring Manager", ""),
            "MD-1":                                        MD1_NAME,
            "MD-2":                                        md2,
            "Status":                                      short_status,
            "Req #":                                       req_num_int,
            "FTE":                                         "",
            "Location":                                    "Crew",
            "Hire Name":                                   hire_name,
            "Start Date":                                  start_date,
            "State":                                       row.get("State", ""),
            "Job Requisition Primary Location (Building)": row.get("Job Requisition Primary Location", ""),
            "Job Requisition Additional Locations":        "",
            "Comment":                                     comment,
        })

    result = pd.DataFrame(rows)
    logger.info(f"Crew Filled complete: {len(result)} rows")
    return result


def build_contractor_unfilled(filtered, ref):
    logger.info("Building Contractor Unfilled...")
    df            = filtered["contractor_open"].copy()
    esf_reqs      = ref["esf_reqs"].copy()
    cc_id         = ref["cc_id"].copy()
    depts         = ref["depts"].copy()
    status_df     = ref["status"].copy()
    prev_req_nums = ref.get("prev_req_nums", set())

    df.columns = [col.replace("\n", " ").strip() for col in df.columns]

    status_map = dict(zip(status_df["status"], status_df["short status"]))

    # ESF Reqs lookup as strings (contractor req numbers may be non-numeric)
    esf_reqs["Req #"] = esf_reqs["Req #"].astype(str).str.strip()
    req_exists_set = set(esf_reqs["Req #"].dropna())

    rows = []
    for idx, row in df.iterrows():
        try:
            req_num = row.get("Req #")
            if pd.isna(req_num):
                logger.debug(f"Skipping row {idx}: missing Req #")
                continue
            req_num_str = str(req_num).strip()
            if not req_num_str:
                logger.debug(f"Skipping row {idx}: empty Req #")
                continue

            # Cost Center -> Department via cc_id
            cost_center = row.get("Cost Center", "")
            if not cost_center:
                logger.debug(f"Row {idx}: empty Cost Center")
                department = ""
            else:
                try:
                    cost_center_num = pd.to_numeric(cost_center, errors="coerce")
                    if pd.isna(cost_center_num):
                        logger.warning(f"Row {idx}: invalid Cost Center '{cost_center}', treating as empty")
                        department = ""
                    else:
                        dept_match = cc_id[cc_id["cc_id"] == cost_center_num]
                        department = dept_match.iloc[0]["subdepartment"] if not dept_match.empty else ""
                except Exception as e:
                    logger.error(f"Row {idx}: error processing Cost Center '{cost_center}': {e}")
                    department = ""

            # Department -> MD-2 via depts
            if not department:
                md2 = ""
            else:
                md2_match = depts[depts["department"] == department]
                md2 = md2_match.iloc[0]["MD-2"] if not md2_match.empty else ""

            if req_num_str not in req_exists_set and req_num_str not in prev_req_nums:
                existing_v_new = "NEW"
            else:
                existing_v_new = "Existing"

            loc = str(row.get("LOC", "")).strip()
            state = {"PA": "Pennsylvania", "TX": "Texas"}.get(loc, loc)

            report_status = row.get("Status (Please Make a Selection from List)", "")
            short_status = status_map.get(str(report_status), "")

            rows.append({
                "Existing v New":                              existing_v_new,
                "Department":                                  department,
                "Worker Type":                                 "Contractor" if cost_center else "",
                "Job Code":                                    "",
                "Job Profile":                                 row.get("Job Title (Standardized)", ""),
                "Cost Center ID":                              cost_center,
                "Grade Level":                                 row.get("Grade Level", ""),
                "Management":                                  row.get("Management", ""),
                "Manager Name":                                row.get("Hiring Manager", ""),
                "MD-1":                                        row.get("MD-1", ""),
                "MD-2":                                        md2,
                "Status":                                      short_status,
                "Req #":                                       req_num_str,
                "FTE":                                         row.get("FTE", ""),
                "Location":                                    row.get("LOC", ""),
                "Note":                                        "",
                "Hire Name":                                   "",
                "Start Date":                                  "",
                "State":                                       state,
                "Job Requisition Primary Location (Building)": "",
                "Job Requisition Additional Locations":        "",
                "Comment":                                     "",
                "Report Status":                               report_status,
            })

        except Exception as e:
            logger.error(f"Error processing row {idx}: {e}")
            continue  # Skip bad row

    result = pd.DataFrame(rows)
    logger.info(f"Contractor Unfilled complete: {len(result)} rows (skipped {len(df) - len(result)})")
    return result


def build_contractor_filled(filtered, ref):
    logger.info("Building Contractor Filled...")
    df        = filtered["contractor_closed"].copy()
    esf_reqs  = ref["esf_reqs"].copy()
    cc_id     = ref["cc_id"].copy()
    depts     = ref["depts"].copy()
    status_df = ref["status"].copy()
    esf_all   = ref["esf_all"].copy()

    # Clean column names FIRST before any filtering
    df.columns = [
        col.replace("\n", " ").replace("\r", " ").strip()
        if isinstance(col, str) else col
        for col in df.columns
    ]
    filled_col = "Filled: F Cancelled : C"
    status_col = "Status (Please Make a Selection from List)"

    # Apply filters:
    # Start Date >= NOW()-10, Filled/Cancelled = "F" (case-insensitive)
    cutoff_date = pd.Timestamp.today() - pd.Timedelta(days=10)
    df["Start Date"] = pd.to_datetime(df["Start Date"], errors="coerce")
    df = df[
        (df["Start Date"] >= cutoff_date) &
        (df[filled_col].astype(str).str.upper().str.strip() == "F")
    ].copy()

    # Status mapping
    status_map = dict(zip(status_df["status"], status_df["short status"]))

    # ESF Reqs lookup as strings (contractor req numbers may be non-numeric)
    def _norm(val) -> str:
        s = str(val).strip()
        return "" if (not s or s.lower() == "nan") else s

    esf_reqs["Req #"] = esf_reqs["Req #"].apply(_norm)
    req_exists_set = set(esf_reqs["Req #"].dropna())
    req_to_hire = (
        esf_reqs.dropna(subset=["Req #"])
        .set_index("Req #")["Hire Name"]
        .to_dict()
        if "Hire Name" in esf_reqs.columns else {}
    )

    # cc_id -> subdepartment
    cc_id["cc_id"] = pd.to_numeric(cc_id["cc_id"], errors="coerce")
    cc_to_subdept = (
        cc_id.dropna(subset=["cc_id"])
        .set_index("cc_id")["subdepartment"]
        .to_dict()
    )

    # department -> MD-2
    dept_to_md2 = (
        depts.dropna(subset=["department"])
        .set_index("department")["MD-2"]
        .to_dict()
    )

    # Infosys override: match Manager Name against ALL "Manager Name" -> Department
    infosys_lookup = (
        esf_all.dropna(subset=["Manager Name"])
        .set_index("Manager Name")["Department"]
        .to_dict()
    )

    rows = []
    for _, row in df.iterrows():
        req_num_str = _norm(row.get("Req #", ""))
        if not req_num_str:
            continue

        cost_center = row.get("Cost Center", "")
        manager     = row.get("Hiring Manager", "")
        start_date  = row.get("Start Date")
        dept_head   = row.get("Dept Head", "")

        # Cost Center -> Department (handle Infosys case)
        try:
            cc_key  = int(str(cost_center).strip()[:4])
            subdept = cc_to_subdept.get(cc_key)
            if subdept is None:
                department = ""
            elif subdept == "Infosys":
                department = infosys_lookup.get(manager, "Infosys")
            else:
                department = subdept
        except (ValueError, TypeError):
            department = ""

        # Department -> MD-2, fallback to Dept Head from ContractorClosed
        md2 = dept_to_md2.get(department)
        md2 = str(md2) if md2 is not None else (str(dept_head) if dept_head else "")

        hire_name = req_to_hire.get(req_num_str, "")

        # "Status" column: new reqs get a validation prompt; known reqs reflect fill state
        if req_num_str not in req_exists_set:
            col_a_status = (
                "Validate if started"
                if pd.notna(start_date) and start_date < pd.Timestamp.today()
                else "NEW"
            )
        else:
            col_a_status = "Newly Filled" if (pd.isna(hire_name) or hire_name == "") else "Filled"

        contractor_status = row.get(status_col, "")
        short_status      = status_map.get(str(contractor_status), "")

        loc   = str(row.get("LOC", "")).strip()
        state = {"PA": "Pennsylvania", "TX": "Texas"}.get(loc, loc)

        rows.append({
            "Existing v New":                              "",
            "Status":                                      col_a_status,
            "Department":                                  department,
            "Worker Type":                                 "Contractor" if cost_center else "",
            "Job Code":                                    "",
            "Job Profile":                                 row.get("Job Tile (Standardized)", ""),
            "Cost Center ID":                              cost_center,
            "Grade Level":                                 row.get("Grade Level", ""),
            "Management":                                  row.get("Management", ""),
            "Manager Name":                                manager,
            "MD-1":                                        row.get("MD-1", ""),
            "MD-2":                                        md2,
            "Req Status":                                  short_status,
            "Req #":                                       req_num_str,
            "FTE":                                         row.get("FTE", ""),
            "Location":                                    row.get("LOC", ""),
            "Note":                                        "",
            "Hire Name":                                   hire_name,
            "Start Date":                                  start_date,
            "State":                                       state,
            "Job Requisition Primary Location (Building)": "",
            "Job Requisition Additional Locations":        "",
            "Comment":                                     "",
            "Contractor Req Status":                       contractor_status,
        })

    result = pd.DataFrame(rows)
    logger.info(f"Contractor Filled complete: {len(result)} rows")
    return result

OUTPUT_FILE = "ESF WF data file.xlsx"
OUTPUT_KEY  = "ESF WF data file.xlsx"
# .ref extension prevents the S3 ObjectCreated trigger (filtered on suffix ".xlsx") from
# firing ses-emailer when rollcall-lambda refreshes the pipeline-internal reference copy.
REF_KEY     = "ESF WF data file.ref"


def _normalize_req(val):
    """Normalize a Req # to a consistent string.

    Pandas reads numeric Excel cells as float64, so integer req IDs written as
    1234567 come back as 1234567.0 and stringify to "1234567.0".  This breaks
    the isin() match against the current run's "1234567" values, causing rows
    to be falsely carried forward or silently dropped.

    Numeric-looking values are converted to int-strings ("1234567.0" → "1234567").
    Non-numeric contractor IDs ("C1234", "CTRC-001") are returned unchanged.
    """
    if pd.isna(val):
        return ""
    s = str(val).strip()
    if not s or s == "nan":
        return ""
    try:
        return str(int(float(s)))
    except (ValueError, TypeError):
        return s

# Master column order for the combined output
MASTER_COLUMNS = [
    "Existing v New",
    "Department",
    "Worker Type",
    "Job Code",
    "Job Profile",
    "Cost Center ID",
    "Grade Level",
    "Management",
    "Manager Name",
    "MD-1",
    "MD-2",
    "Status",
    "Req #",
    "FTE",
    "Location",
    "Note",
    "Hire Name",
    "Start Date",
    "State",
    "Job Requisition Primary Location (Building)",
    "Job Requisition Additional Locations",
    "Comment",
    "Report Status",
    "Contractor Req Status",
    "Req Status",
]

def standardize_df(df):
    rename_map = {
        "Cost Center":     "Cost Center ID",
        "Grade level":     "Grade Level",
        "Management Type": "Management",
    }
    df = df.rename(columns=rename_map)
    for col in MASTER_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[MASTER_COLUMNS]

def write_output_workbook(crew_unfilled, crew_filled, contractor_unfilled, contractor_filled, ret_addr, ref=None, orig_message_id="", orig_subject=""):
    logger.info("Writing output workbook...")
    output_path = os.path.join(TMP_DIR, OUTPUT_FILE)

    # Step 1 — load previous run from S3
    try:
        logger.info("Loading previous ESF WF data file from S3...")
        response = s3.get_object(Bucket=DEPTS_BUCKET, Key=REF_KEY)
        esf_bytes = io.BytesIO(response["Body"].read())
        
        # Check which sheet exists — first run will have "Reqs", subsequent runs "Output"
        xl = pd.ExcelFile(esf_bytes)
        if "Output" in xl.sheet_names:
            prev_df = pd.read_excel(xl, sheet_name="Output")
            logger.info("Found 'Output' sheet from previous run")
        elif "Reqs" in xl.sheet_names:
            prev_df = pd.read_excel(xl, sheet_name="Reqs")
            logger.info("Found 'Reqs' sheet — treating as first run")
        else:
            logger.info("No recognized sheet found — starting fresh")
            prev_df = pd.DataFrame(columns=MASTER_COLUMNS)

        prev_df["Req #"] = prev_df["Req #"].map(_normalize_req)
        logger.info(f"Loaded {len(prev_df)} rows from previous run")
    except s3.exceptions.NoSuchKey:
        # Fresh deploy — bootstrap from the static ref file (Reqs sheet) if it exists
        try:
            logger.info("No reference file found — bootstrapping from %s", ESF_WF_FILE)
            response = s3.get_object(Bucket=DEPTS_BUCKET, Key=ESF_WF_FILE)
            esf_bytes = io.BytesIO(response["Body"].read())
            xl = pd.ExcelFile(esf_bytes)
            if "Reqs" in xl.sheet_names:
                prev_df = pd.read_excel(xl, sheet_name="Reqs")
                prev_df["Req #"] = prev_df["Req #"].map(_normalize_req)
                logger.info("Bootstrapped %d rows from %s Reqs sheet", len(prev_df), ESF_WF_FILE)
            else:
                logger.info("No Reqs sheet in bootstrap file — starting fresh")
                prev_df = pd.DataFrame(columns=MASTER_COLUMNS)
        except s3.exceptions.NoSuchKey:
            logger.info("No bootstrap file found — starting fresh")
            prev_df = pd.DataFrame(columns=MASTER_COLUMNS)

    # Step 2 — standardize and combine new data
    new_df = pd.concat([
        standardize_df(crew_unfilled),
        standardize_df(crew_filled),
        standardize_df(contractor_unfilled),
        standardize_df(contractor_filled),
    ], ignore_index=True)
    new_df["Req #"] = new_df["Req #"].map(_normalize_req)
    logger.info(f"New data: {len(new_df)} rows")

    # Step 3 — merge: new data takes precedence; rows no longer in reports are carried forward
    new_req_nums = set(new_df["Req #"].dropna())

    carried_forward = prev_df[~prev_df["Req #"].isin(new_req_nums)].copy()
    carried_forward["Existing v New"] = "Carried Forward"
    logger.info(f"Carried forward from previous run: {len(carried_forward)} rows")

    combined = pd.concat([new_df, carried_forward], ignore_index=True)
    for col in MASTER_COLUMNS:
        if col not in combined.columns:
            combined[col] = ""
    combined = combined[MASTER_COLUMNS]
    logger.info(f"Combined total: {len(combined)} rows")

    # Step 4 — write and upload
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        combined.to_excel(writer, sheet_name="Output", index=False)

    logger.info(f"Output workbook written to '{output_path}'")
    key = f"{ret_addr}/{OUTPUT_KEY}"
    s3_meta = {k: v for k, v in {
        "original-message-id": orig_message_id,
        "original-subject":    orig_subject[:500],  # S3 metadata value limit is 2 KB total
    }.items() if v}
    s3.upload_file(output_path, DEPTS_BUCKET, key, ExtraArgs={"Metadata": s3_meta} if s3_meta else {})
    logger.info(f"Uploaded output to s3://{DEPTS_BUCKET}/{key}")

    s3.upload_file(output_path, DEPTS_BUCKET, REF_KEY)
    logger.info(f"Updated reference copy at s3://{DEPTS_BUCKET}/{REF_KEY}")

    return output_path

def _send_failure_email(to_email, subject_ref="", error_msg=""):
    if not to_email or not SENDER_EMAIL:
        logger.warning("Cannot send failure notification — sender or recipient email not configured")
        return
    try:
        ref = f' "{subject_ref}"' if subject_ref else ""
        body = (
            f"Your files{ref} were received, but an error occurred while generating "
            "the reconciliation report. No output workbook was delivered.\n\n"
        )
        if error_msg:
            body += f"Error: {error_msg}\n\n"
        body += (
            "Please resubmit your files. If the problem continues, contact your administrator."
        )
        ses.send_email(
            Source=SENDER_EMAIL,
            Destination={"ToAddresses": [to_email]},
            Message={
                "Subject": {"Data": "Pipeline Error — Report Generation Failed"},
                "Body":    {"Text": {"Data": body}},
            },
        )
        logger.info("Failure notification sent to %s", to_email)
    except Exception as e:
        logger.error("Could not send failure notification to %s: %s", to_email, e)


def lambda_handler(event, context):
    start = time.time()
    logger.info("Lambda handler invoked")
    logger.info(f"Event: {json.dumps(event, indent=2)}")

    ret_addr        = None
    orig_message_id = ""
    orig_subject    = ""
    try:
        sns_payload     = json.loads(json.loads(event["Records"][0].get("body", "{}")).get("Message", "{}"))
        ret_addr        = sns_payload.get("retAddr")
        orig_message_id = sns_payload.get("origMessageId", "")
        orig_subject    = sns_payload.get("origSubject", "")
        logger.info("Return address: %s | origMessageId: %s | origSubject: %s", ret_addr, orig_message_id, orig_subject)
        discovered  = discover_files(BUCKET_NAME)
        local_files = download_all_files(BUCKET_NAME, discovered)
        filtered    = filter_all_files(local_files)

        dept_codes = load_dept_codes(DEPTS_BUCKET, CC_ID_FILE)
        filtered["candidates"] = filter_candidates(local_files["candidates"], dept_codes)

        ref = load_reference_data(DEPTS_BUCKET)

        crew_unfilled       = build_crew_unfilled(filtered, ref)
        crew_filled         = build_crew_filled(filtered, ref)
        contractor_unfilled = build_contractor_unfilled(filtered, ref)
        contractor_filled   = build_contractor_filled(filtered, ref)

        output_path = write_output_workbook(
            crew_unfilled, crew_filled, contractor_unfilled, contractor_filled, ret_addr,
            ref=ref, orig_message_id=orig_message_id, orig_subject=orig_subject,
        )

        elapsed = time.time() - start
        logger.info(f"Process complete in {elapsed:.2f}s. Output written to: {output_path}")
        return {
            "statusCode": 200,
            "status": "success",
            "body": json.dumps({
                "message": "Headcount reconciliation complete",
                "retAddr": ret_addr,
                "bucket": DEPTS_BUCKET,
                "key": OUTPUT_KEY
            })
        }

    except Exception as e:
        logger.error("Unhandled exception: %s", e, exc_info=True)
        _send_failure_email(ret_addr, orig_subject, error_msg=str(e))
        raise
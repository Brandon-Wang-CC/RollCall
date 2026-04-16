import json
import logging
import random

import boto3
import csv
import io
import pymysql
import botocore.exceptions
from datetime import datetime, timedelta

# =========================
# Logging
# =========================
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# =========================
# AWS Clients
# =========================
s3 = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")


# =========================
# Secrets Manager
# =========================
import json
import botocore.exceptions
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_db_credentials():
    secret_name = "rollcall-db-secret"

    try:
        logger.info(f"Requesting secret '{secret_name}' from Secrets Manager...")
        response = secrets_client.get_secret_value(SecretId=secret_name)
        logger.info("Successfully retrieved secret value.")

    except botocore.exceptions.EndpointConnectionError as e:
        logger.error("Could not reach Secrets Manager endpoint. "
                     "This usually means the Lambda has no route to Secrets Manager "
                     "(missing VPC endpoint or NAT).")
        raise

    except botocore.exceptions.ClientError as e:
        error_code = e.response["Error"]["Code"]

        if error_code == "AccessDeniedException":
            logger.error("Lambda does not have permission to read the secret.")
        elif error_code == "ResourceNotFoundException":
            logger.error(f"Secret '{secret_name}' does not exist.")
        else:
            logger.error(f"Secrets Manager ClientError: {error_code}")

        raise

    except Exception as e:
        logger.error(f"Unexpected error retrieving secret: {str(e)}")
        raise

    # Parse the secret JSON
    try:
        secret_dict = json.loads(response["SecretString"])
    except Exception as e:
        logger.error("SecretString is not valid JSON.")
        raise

    # Validate required fields
    required_fields = ["host", "username", "password"]
    for field in required_fields:
        if field not in secret_dict:
            logger.error(f"Secret is missing required field: '{field}'")
            raise KeyError(f"Missing field '{field}' in secret")

    return {
        "host": secret_dict["host"],
        "user": secret_dict["username"],
        "password": secret_dict["password"],
        "database": secret_dict.get("dbname", "Main"),
        "port": secret_dict.get("port", 3306)
    }

# =========================
# Validation
# =========================
def validate_row(row: dict) -> list:
    errors = []
    return errors

def get_three_days_pre_monday() -> datetime:
    """
    Calculate the date that is 3 days before the last Monday. For Contractor Filled logic.
    
    Example: If today is Thursday Apr 18, 2026:
    - Last Monday was Apr 13
    - 3 days before Monday = Friday Apr 10
    """
    today = datetime.now()
    # Calculate days since last Monday (0=Monday, 6=Sunday)
    days_since_monday = (today.weekday() - 0) % 7
    last_monday = today - timedelta(days=days_since_monday)
    three_days_pre_monday = last_monday - timedelta(days=3)
    return three_days_pre_monday

# =========================
# Transformation
# =========================
def transform_row(row: dict) -> dict:
    if 'StartDate' in row and row['StartDate']:
        row['StartDate'] = row['StartDate']
    blank_val = "BLANK"
    newRow = {}

    newRow["Department"] = row.get("Dept", blank_val)
    newRow["WorkerType"] = row.get("Worker Type", blank_val)
    newRow["JobCode"] = row.get("JobCode", blank_val)
    newRow["JobProfile"] = row.get("Job Profile", blank_val)
    newRow["CostCenter"] = row.get("Cost Center ID", blank_val)
    newRow["GradeLevel"] = row.get("GradeLevel", blank_val)
    newRow["Management"] = row.get("Management", blank_val)
    newRow["ManagerName"] = row.get("ManagerName", blank_val)
    newRow["MD1"] = row.get("MD1", blank_val)
    newRow["MD2"] = row.get("MD2", blank_val)
    newRow["Status"] = row.get("Status", blank_val)
    newRow["ReqNumber"] = row.get("ReqNumber", random.randint(0,9999999))
    newRow["HireName"] = row.get("Employee Name", blank_val)
    newRow["StartDate"] = row.get("StartDate", blank_val)
    newRow["State"] = row.get("State", blank_val)
    newRow["PrimaryLocation"] = row.get("Work Location", blank_val)
    newRow["AdditionalLocations"] = row.get("AdditionalLocations", blank_val)
    newRow["Comment"] = row.get("Comment", blank_val)
    return newRow

# =========================
# S3
# =========================
def download_csv(bucket: str, key: str) -> str:
    logger.info(f"Downloading file from S3: {bucket}/{key}")
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    logger.info("File downloaded successfully")
    return content

def parse_csv(content: str) -> list:
    reader = csv.DictReader(io.StringIO(content))
    return list(reader)

# =========================
# DeptsSheet Turned into a Dictionary
# =========================
departments_lookup = {
    "1083": "Admin",
    "1734": "Admin",
    "1011": "CSOC",
    "890": "Europe",
    "826": "GES",
    "827": "GES",
    "1018": "GES",
    "1068": "GES",
    "1075": "GES",
    "1076": "GES",
    "1081": "GES",
    "1096": "GES",
    "981": "GS&F",
    "1176": "GS&F",
    "1484": "GS&F",
    "437": "IAM",
    "460": "IAM",
    "1575": "IAM",
    "1576": "IAM",
    "1577": "IAM",
    "1578": "IAM",
    "1579": "IAM",
    "2431": "IAM",
    "1293": "Infosys",
    "1483": "Infosys",
    "1706": "Infosys",
    "973": "Infosys",
    "1077": "SAEP",
    "1078": "SAEP",
    "1079": "SAEP",
    "1080": "SAEP",
    "1117": "SAEP",
    "1084": "VIA",
    "0808": "* NOT ES&F *",
    "808": "* NOT ES&F *"
}

# =========================
# Contractor Filled Logic
# =========================
def contractor_filled(rows: dict) -> dict:
    newRow = {}   
    # Department Check
    if rows.get("CostCenter") is not None:
        if departments_lookup.get(rows.get("CostCenter")) == "Infosys":  #Test Case: What if cost center is a random number that isnt in the table?
            print("Still working")
        else:
            newRow["Department"] = departments_lookup.get(rows.get("CostCenter"))
    else:
        newRow["Department"] = ""
        
    #Worker Type Check
    if rows.get("WorkerType", "") != "":  #Test Case: What if this is something random that isn't blank or contractor?
        newRow["WorkerType"] = "Contractor"
    else:
        newRow["WorkerType"] = ""

    #Job Profile Check
    #Cost Center Check
    #Grade Level Check
    if rows.get("GradeLevel", "") != "":  #Test Case: What if this is something random that isn't blank?
        newRow["GradeLevel"] = "00"
    else:
        newRow["GradeLevel"] = ""

    #Management Check
    if rows.get("Management", "") != "":  #Test Case: What if this is something random that isn't blank or Non-Management?
        newRow["Management"] = "Non-Management"
    else:
        newRow["Management"] = ""
    #Manager Name Check
    #MD1 Check
    if rows.get("MD1", "") != "":  #Test Case: What if this is something random that isn't blank or Manish Nagar (019067)?
        newRow["MD1"] = "Manish Nagar (019067)"
    else:
        newRow["MD1"] = ""
    #MD2 Check
    #Req Number Check
    #FTE Check Pending
    #Hire Name Check
    #Start Date Check
    #State Check
    #Contractor Req Status Check
    #Potential Name Check?

    # Status Check
    date_for_contractor = get_three_days_pre_monday()
    req_number_current = newRow.get("ReqNumber", "")
    start_date = rows.get("StartDate", "")
    
    if not req_number_current:
        newRow["Status"] = ""
        return newRow
    
    dept = rows.get("Department")

    # ISERROR(VLOOKUP()) Look up current ReqNumber in ESF file and grab the department value and see if its valid
    if dept is None: #If there is an error with department value
        if start_date < date_for_contractor:
            newRow["Status"] = "Validate if started"
        else:
            newRow["Status"] = "NEW"
    else:
        if rows.get("CostCenter", "") == "":
            newRow["Status"] = "Newly Filled"
        else:
            newRow["Status"] = "Filled"

    return newRow




# =========================
# Database
# =========================
def test_database_connection():
    try:
        logger.info("Fetching DB credentials from Secrets Manager...")

        creds = get_db_credentials()

        # SAFE logging (no password)
        logger.info(f"DB Host: {creds['host']}")
        logger.info(f"DB User: {creds['user']}")
        logger.info(f"DB Name: {creds['database']}")
        logger.info(f"DB Port: {creds['port']}")

        logger.info("Attempting DB connection...")

        conn = pymysql.connect(
            host=creds["host"],
            user=creds["user"],
            password=creds["password"],
            database=creds["database"],
            port=creds["port"],
            connect_timeout=10
        )

        logger.info("Connection established. Running test query...")

        with conn.cursor() as cursor:
            cursor.execute("SELECT 1;")
            result = cursor.fetchone()

        logger.info(f"Test query result: {result}")

        conn.close()

        logger.info("DB connection closed successfully.")

        return {
            "statusCode": 200,
            "body": f"Database connection successful: {result}"
        }

    except Exception as e:
        logger.error(f"DB connection failed: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Database connection failed: {str(e)}"
        }

def load_into_db(rows: list):
    if not rows:
        logger.info("No valid rows to insert")
        return

    creds = get_db_credentials()

    conn = pymysql.connect(
        host=creds["host"],
        user=creds["user"],
        password=creds["password"],
        database=creds["database"],
        port=creds["port"],
        connect_timeout=5
    )

    try:
        with conn.cursor() as cursor:
            # Insert rows
            for row in rows:
                cursor.execute("""
                    INSERT INTO Main 
                    (Department, WorkerType, JobCode, JobProfile, CostCenter, GradeLevel,
                     Management, ManagerName, MD1, MD2, Status, ReqNumber, HireName, StartDate,
                     State, PrimaryLocation, AdditionalLocations, Comment)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row.get("Department"),
                    row.get("WorkerType"),
                    row.get("JobCode"),
                    row.get("JobProfile"),
                    row.get("CostCenter"),
                    row.get("GradeLevel"),
                    row.get("Management"),
                    row.get("ManagerName"),
                    row.get("MD1"),
                    row.get("MD2"),
                    row.get("Status"),
                    row.get("ReqNumber"),
                    row.get("HireName"),
                    row.get("StartDate"),
                    row.get("State"),
                    row.get("PrimaryLocation"),
                    row.get("AdditionalLocations"),
                    row.get("Comment")
                ))

            # Commit inserts
            conn.commit()

            # Now print all rows from Main
            cursor.execute("SELECT * FROM Main;")
            all_rows = cursor.fetchall()
            logger.info("=== Full Main Table ===")
            for r in all_rows:
                logger.info(r)
            logger.info("=======================")

    except Exception as e:
        logger.error(f"DB operation failed: {str(e)}")
        raise
    finally:
        conn.close()

# =========================
# Lambda Handler
# =========================
def lambda_handler(event, context):

    logger.info(f"Event received: {event}")

    # Special test mode
    if event.get("action") == "test_db_connection":
        return test_database_connection()

    for record in event.get("Records", []):
        body = json.loads(record["body"])

        bucket = body.get("bucket")
        file_key = body.get("object_key")

        if not bucket and not file_key:
            logger.error("Missing bucket and file name")
            continue
        if not bucket:
            logger.error("Missing bucket")
            continue
        if not file_key:
            logger.error("Missing file name")
            continue

        try:
            content = download_csv(bucket, file_key)
            raw_rows = parse_csv(content)

            valid_rows = []
            invalid_rows = []

            for row in raw_rows:
                errors = validate_row(row)

                if errors:
                    invalid_rows.append({"row": row, "errors": errors})
                else:  # Apply contractor filled logic and transform
                    transformed = transform_row(row)
                    valid_rows.append(transformed)

            logger.info(f"Valid rows: {len(valid_rows)}")
            logger.info(f"Invalid rows: {len(invalid_rows)}")

            load_into_db(valid_rows)

        except Exception as e:
            logger.error(f"Processing failed: {str(e)}")
            raise e

    return {
        "statusCode": 200,
        "body": "File processed successfully"
    }
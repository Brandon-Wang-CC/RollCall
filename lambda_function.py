import json
import logging
import boto3
import csv
import io
import pymysql
import botocore.exceptions

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

# =========================
# Transformation
# =========================
def transform_row(row: dict) -> dict:
    if 'StartDate' in row and row['StartDate']:
        row['StartDate'] = row['StartDate']
    return row

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
        file_key = body.get("file")

        if not bucket or not file_key:
            logger.error("Missing bucket or file name")
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
                else:
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
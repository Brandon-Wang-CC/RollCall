import json
import logging
import boto3
import csv
import io
import pymysql


logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client("s3")

# =========================
# Validation Functions
# =========================

def validate_row(row: dict) -> list:
    """
    Validate required fields and formats.
    Returns list of error messages (empty if valid).
    """
    errors = []

    return errors


# =========================
# Transformation Functions
# =========================

def transform_row(row: dict) -> dict:
    """
    Transform raw CSV row into cleaned/normalized format.
    """

    return None


# =========================
# S3 Functions
# =========================

def download_csv(bucket: str, key: str) -> str:
    """
    Download CSV file from S3 and return file content as string.
    """
    logger.info(f"Downloading file from S3: {bucket}/{key}")

    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")

    logger.info("File downloaded successfully")
    return content


def parse_csv(content: str) -> list:
    """
    Parse CSV string into list of dictionaries.
    """
    reader = csv.DictReader(io.StringIO(content))
    return list(reader)


# =========================
# Database Functions (Skeleton)
# =========================

def load_into_db(rows: list):
    """
    Insert transformed rows into database.
    """


# =========================
# Lambda Handler
# =========================

def lambda_handler(event, context):
    logger.info("Lambda triggered")

    for record in event.get("Records", []):
        body = json.loads(record["body"])

        bucket = body.get("bucket")
        file_key = body.get("file")

        logger.info(f"Bucket: {bucket}")
        logger.info(f"File: {file_key}")

        if not bucket or not file_key:
            logger.error("Missing bucket or file name")
            continue

        try:
            # 1. Download
            content = download_csv(bucket, file_key)

            # 2. Parse
            raw_rows = parse_csv(content)

            valid_rows = []
            invalid_rows = []

            # 3. Validate + Transform
            for row in raw_rows:
                errors = validate_row(row)

                if errors:
                    invalid_rows.append({
                        "row": row,
                        "errors": errors
                    })
                else:
                    transformed = transform_row(row)
                    valid_rows.append(transformed)

            logger.info(f"Valid rows: {len(valid_rows)}")
            logger.info(f"Invalid rows: {len(invalid_rows)}")

            # 4. Load to DB
            load_into_db(valid_rows)

        except Exception as e:
            logger.error(f"Processing failed: {str(e)}")
            raise e

    return {
        "statusCode": 200,
        "body": "File processed successfully"
    }
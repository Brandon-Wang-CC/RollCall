import json
import os
import time
import boto3
import logging
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import urllib.parse

s3 = boto3.client("s3")
ses = boto3.client("ses")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def parse_event(event):
    record = event["Records"][0]
    body = record.get("body")
    return json.loads(body) if isinstance(body, str) else body


def download_file_from_s3(bucket, key, local_path="/tmp/report.xlsx"):
    logger.info(f"Downloading s3://{bucket}/{key} → {local_path}")
    s3.download_file(bucket, key, local_path)
    size_kb = os.path.getsize(local_path) / 1024
    logger.info(f"Downloaded {size_kb:.1f} KB")
    return local_path


def send_email_with_attachment(to_email, subject, body, file_path, filename=None):
    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = os.environ.get("SENDER_EMAIL")
    msg["To"] = to_email

    # Body
    msg.attach(MIMEText(body, "plain"))

    # Attachment
    with open(file_path, "rb") as f:
        part = MIMEApplication(f.read(), _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    part.add_header(
        "Content-Disposition",
        "attachment",
        filename=filename or file_path.split("/")[-1]
    )

    msg.attach(part)

    return ses.send_raw_email(
        Source=msg["From"],
        Destinations=[to_email],
        RawMessage={"Data": msg.as_string()}
    )


def lambda_handler(event, context):
    start = time.time()
    logger.info(f"Event: {json.dumps(event, indent=2)}")
    logger.info(f"SENDER_EMAIL: {os.environ.get('SENDER_EMAIL')}")

    record = event["Records"][0]

    # S3 bucket + key
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    # S3 URL encoding fix
    key = urllib.parse.unquote_plus(key)

    if "/" not in key:
        logger.info(f"Skipping non-output key (no email prefix): {key}")
        return {"statusCode": 200, "body": json.dumps({"skipped": key})}

    to_email = key.split("/")[0]
    file_key = key.split("/")[1]

    logger.info(f"Recipient: {to_email} | File: {file_key} | Source: s3://{bucket}/{key}")

    subject = "Pipeline Complete"
    body = "See attached file."

    timestamp = datetime.now().strftime("%B %-d %Y %-I-%M %p")
    attachment_name = f"ESF WF data file {timestamp}.xlsx"

    # 1. download from S3
    local_file = download_file_from_s3(bucket, key)
    logger.info(f"Downloaded file to {local_file}")

    # 2. send email
    response = send_email_with_attachment(
        to_email=to_email,
        subject=subject,
        body=body,
        file_path=local_file,
        filename=attachment_name,
    )
    elapsed = time.time() - start
    logger.info(f"Email sent to {to_email} | SES MessageId: {response['MessageId']} | elapsed: {elapsed:.2f}s")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Email sent successfully",
            "to": to_email,
            "s3": f"s3://{bucket}/{key}",
            "sesMessageId": response["MessageId"]
        })
    }
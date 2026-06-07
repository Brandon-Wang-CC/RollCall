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


def _notify_failure(to_email, error_msg):
    if not to_email:
        logger.warning("Cannot send failure notification — recipient email not yet known")
        return
    try:
        ses.send_email(
            Source=os.environ.get("SENDER_EMAIL", ""),
            Destination={"ToAddresses": [to_email]},
            Message={
                "Subject": {"Data": "RollCall pipeline error"},
                "Body": {"Text": {"Data": (
                    f"Your RollCall pipeline run failed in ses-emailer.\n\n"
                    f"Error: {error_msg}\n\n"
                    f"Check CloudWatch logs (/aws/lambda/ses-emailer-function) for full details."
                )}},
            },
        )
        logger.info("Failure notification sent to %s", to_email)
    except Exception as e:
        logger.error("Could not send failure notification to %s: %s", to_email, e)


def _handle_failure_event(record):
    """Receive a pipeline failure event from SQS and send a polite failure email to the original sender."""
    try:
        failure_event = json.loads(record["body"])
    except Exception as e:
        logger.error("Could not parse failure event body: %s", e)
        raise

    sender      = failure_event.get("sender", "")
    failed_in   = failure_event.get("failedIn", "unknown")
    timestamp   = failure_event.get("timestamp", "")
    error       = failure_event.get("error", "An unexpected error occurred.")
    orig_subject = failure_event.get("subject", "")

    if not sender:
        logger.warning("No sender email in failure event — skipping notification")
        return {"statusCode": 200, "body": json.dumps({"skipped": "no sender"})}

    subject_ref = f'"{orig_subject}"' if orig_subject else "your pipeline run"
    subject_line = f"RollCall Pipeline Error — {subject_ref}"

    body_lines = [
        "Hello,",
        "",
        f"Unfortunately, {subject_ref} could not be completed.",
        "",
        "Details:",
    ]
    if orig_subject:
        body_lines.append(f"  Original email:  {orig_subject}")
    body_lines += [
        f"  Failed in:       {failed_in}",
        f"  Time:            {timestamp}",
        f"  Details:         {error}",
        "",
        "If this continues, please contact your administrator.",
        "",
        "— RollCall",
    ]

    logger.info("Sending failure notification to %s (failedIn: %s)", sender, failed_in)
    ses.send_email(
        Source=os.environ.get("SENDER_EMAIL", ""),
        Destination={"ToAddresses": [sender]},
        Message={
            "Subject": {"Data": subject_line},
            "Body":    {"Text": {"Data": "\n".join(body_lines)}},
        },
    )
    logger.info("Failure notification sent to %s", sender)
    return {"statusCode": 200, "body": json.dumps({"notified": sender, "failedIn": failed_in})}


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

    msg.attach(MIMEText(body, "plain"))

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

    # SQS failure queue → send failure notification email (no recursive _notify_failure)
    if record.get("eventSource") == "aws:sqs":
        return _handle_failure_event(record)

    to_email = None
    try:
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        # S3 event notifications encode special chars (e.g. @ → %40) in object keys
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

        local_file = download_file_from_s3(bucket, key)
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

    except Exception as e:
        logger.error("Unhandled exception: %s", e, exc_info=True)
        _notify_failure(to_email, str(e))
        raise
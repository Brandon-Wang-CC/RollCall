import importlib.util
import io
import json
import os

# Must be set before any Lambda module is imported — they create boto3 clients at module level.
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

import openpyxl
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_module(label, relpath):
    """Import a lambda file by path with a unique module name to avoid collisions."""
    path = os.path.join(ROOT, relpath)
    spec = importlib.util.spec_from_file_location(label, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_xlsx_bytes(sheet_names=None, rows=None):
    """Return bytes of a minimal .xlsx workbook."""
    wb = openpyxl.Workbook()
    if sheet_names:
        wb.remove(wb.active)
        for name in sheet_names:
            ws = wb.create_sheet(name)
            if rows:
                for row in rows:
                    ws.append(row)
    elif rows:
        ws = wb.active
        for row in rows:
            ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def make_raw_email_bytes(attachments=None):
    """Return a BytesIO of a MIME email with optional XLSX attachments.

    attachments: list of (filename, bytes) tuples
    """
    msg = MIMEMultipart()
    msg["From"] = "sender@example.com"
    msg["To"] = "pipeline@example.com"
    msg["Subject"] = "Report"
    for filename, content in (attachments or []):
        part = MIMEApplication(content, _subtype="xlsx")
        part.add_header("Content-Disposition", "attachment", filename=filename)
        msg.attach(part)
    return io.BytesIO(msg.as_bytes())


def make_csv_parser_event(sender="sender@example.com",
                          bucket="test-ses-bucket",
                          key="RAW_EMAIL/msg-abc"):
    """SQS event wrapping an SNS notification from SES email receiving."""
    inner = json.dumps({
        "mail": {"source": sender},
        "receipt": {"action": {"bucketName": bucket, "objectKey": key}},
    })
    return {"Records": [{"body": json.dumps({"Message": inner})}]}


def make_rollcall_event(ret_addr="sender@example.com"):
    """SQS event wrapping an SNS notification published by csvParser."""
    inner = json.dumps({"retAddr": ret_addr})
    return {"Records": [{"body": json.dumps({"Message": inner})}]}


def make_ses_emailer_event(key="sender%40example.com/report+output+file.xlsx",
                           bucket="test-depts-bucket"):
    """S3 ObjectCreated event that triggers ses-emailer."""
    return {"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": key}}}]}

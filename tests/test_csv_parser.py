import io
import json
import os
from unittest.mock import MagicMock, patch, call

import pytest

from conftest import load_module, make_xlsx_bytes, make_raw_email_bytes, make_csv_parser_event

csv_parser = load_module("csv_parser", "lambdas/csv-parser/theLambda.py")


# ── Helpers ───────────────────────────────────────────────────────────────────

def mock_s3_get(raw_email_bytes):
    """Return a mock s3.get_object response whose Body is a file-like object."""
    m = MagicMock()
    m.get_object.return_value = {"Body": raw_email_bytes}
    return m


# ── process_file ──────────────────────────────────────────────────────────────

def test_process_file_skips_non_xlsx():
    with patch.object(csv_parser, "s3", MagicMock()):
        result = csv_parser.process_file("report.csv", b"data")
    assert result == []


def test_process_file_default_copies_xlsx():
    mock_s3 = MagicMock()
    xlsx = make_xlsx_bytes()
    with patch.object(csv_parser, "s3", mock_s3):
        result = csv_parser.process_file("Unfilled Reqs.xlsx", xlsx)
    assert len(result) == 1
    assert result[0].endswith(".xlsx")
    assert "Unfilled Reqs-copy-" in result[0]
    mock_s3.put_object.assert_called_once()
    call_kwargs = mock_s3.put_object.call_args.kwargs
    assert call_kwargs["Key"] == result[0]


def test_process_file_new_it_splits_open_and_closed():
    mock_s3 = MagicMock()
    xlsx = make_xlsx_bytes(sheet_names=["Open", "Closed"])
    filename = "NEW-IT Contractor-VG-Vendor Req Report 2024-01.xlsx"
    with patch.object(csv_parser, "s3", mock_s3):
        result = csv_parser.process_file(filename, xlsx)
    assert len(result) == 2
    assert any("Open" in k for k in result)
    assert any("Closed" in k for k in result)
    assert mock_s3.put_object.call_count == 2


def test_process_file_new_it_skips_missing_sheet():
    mock_s3 = MagicMock()
    # Only has Open sheet, no Closed
    xlsx = make_xlsx_bytes(sheet_names=["Open"])
    filename = "NEW-IT Contractor-VG-Vendor Req Report.xlsx"
    with patch.object(csv_parser, "s3", mock_s3):
        result = csv_parser.process_file(filename, xlsx)
    assert len(result) == 1
    assert any("Open" in k for k in result)
    assert mock_s3.put_object.call_count == 1


def test_process_file_returns_empty_list_on_error():
    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = Exception("S3 error")
    with patch.object(csv_parser, "s3", mock_s3):
        result = csv_parser.process_file("report.xlsx", make_xlsx_bytes())
    assert result == []


# ── extract_sheet_to_xlsx_bytes ───────────────────────────────────────────────

def test_extract_sheet_found_returns_bytes():
    xlsx = make_xlsx_bytes(sheet_names=["Open", "Closed"])
    result = csv_parser.extract_sheet_to_xlsx_bytes(xlsx, "Open")
    assert isinstance(result, bytes)
    assert len(result) > 0


def test_extract_sheet_not_found_returns_none():
    xlsx = make_xlsx_bytes(sheet_names=["Open"])
    result = csv_parser.extract_sheet_to_xlsx_bytes(xlsx, "Missing")
    assert result is None


def test_extract_sheet_bad_bytes_returns_none():
    result = csv_parser.extract_sheet_to_xlsx_bytes(b"not an xlsx", "Open")
    assert result is None


# ── wipe_buckets ──────────────────────────────────────────────────────────────

def test_wipe_buckets_logs_deleted_count(caplog):
    mock_bucket = MagicMock()
    mock_bucket.objects.all.return_value.delete.return_value = [
        {"Deleted": [{"Key": "a.xlsx"}, {"Key": "b.xlsx"}]}
    ]
    mock_resource = MagicMock()
    mock_resource.Bucket.return_value = mock_bucket
    with patch("boto3.resource", return_value=mock_resource), \
         patch.object(csv_parser, "CSV_BUCKET", "test-csv-bucket"):
        with caplog.at_level("INFO"):
            csv_parser.wipe_buckets()
    assert "2" in caplog.text


def test_wipe_buckets_handles_empty_bucket():
    mock_bucket = MagicMock()
    mock_bucket.objects.all.return_value.delete.return_value = []
    mock_resource = MagicMock()
    mock_resource.Bucket.return_value = mock_bucket
    with patch("boto3.resource", return_value=mock_resource):
        csv_parser.wipe_buckets()  # should not raise


# ── lambda_handler ────────────────────────────────────────────────────────────

def test_lambda_handler_blocked_sender_returns_early():
    event = make_csv_parser_event(sender="no-reply-aws@amazon.com")
    mock_s3 = MagicMock()
    mock_bucket = MagicMock()
    mock_bucket.objects.all.return_value.delete.return_value = []
    with patch("boto3.resource", return_value=MagicMock(Bucket=lambda _: mock_bucket)), \
         patch.object(csv_parser, "s3", mock_s3), \
         patch.object(csv_parser, "sns_client", MagicMock()):
        result = csv_parser.lambda_handler(event, {})
    assert result["status"] == "ignored"
    mock_s3.get_object.assert_not_called()


def test_lambda_handler_processes_xlsx_attachment():
    xlsx = make_xlsx_bytes(rows=[["col1", "col2"], ["val1", "val2"]])
    raw_email = make_raw_email_bytes([("Unfilled Reqs.xlsx", xlsx)])
    event = make_csv_parser_event()

    mock_s3 = mock_s3_get(raw_email)
    mock_bucket = MagicMock()
    mock_bucket.objects.all.return_value.delete.return_value = []
    mock_sns = MagicMock()
    mock_sns.publish.return_value = {"MessageId": "msg-123"}

    with patch("boto3.resource", return_value=MagicMock(Bucket=lambda _: mock_bucket)), \
         patch.object(csv_parser, "s3", mock_s3), \
         patch.object(csv_parser, "sns_client", mock_sns):
        result = csv_parser.lambda_handler(event, {})

    assert result["status"] == "success"
    assert result["total_files"] == 1
    assert len(result["processed_files"]) == 1
    mock_sns.publish.assert_called_once()


def test_lambda_handler_skips_non_xlsx_attachment():
    raw_email = make_raw_email_bytes([("notes.txt", b"some text")])
    event = make_csv_parser_event()

    mock_s3 = mock_s3_get(raw_email)
    mock_bucket = MagicMock()
    mock_bucket.objects.all.return_value.delete.return_value = []
    mock_sns = MagicMock()
    mock_sns.publish.return_value = {"MessageId": "msg-123"}

    with patch("boto3.resource", return_value=MagicMock(Bucket=lambda _: mock_bucket)), \
         patch.object(csv_parser, "s3", mock_s3), \
         patch.object(csv_parser, "sns_client", mock_sns):
        result = csv_parser.lambda_handler(event, {})

    assert result["status"] == "success"
    assert result["total_files"] == 1
    assert result["processed_files"] == []
    mock_sns.publish.assert_called_once()


def test_lambda_handler_publishes_sender_as_ret_addr():
    raw_email = make_raw_email_bytes()
    event = make_csv_parser_event(sender="user@example.com")

    mock_s3 = mock_s3_get(raw_email)
    mock_bucket = MagicMock()
    mock_bucket.objects.all.return_value.delete.return_value = []
    mock_sns = MagicMock()
    mock_sns.publish.return_value = {"MessageId": "msg-123"}

    with patch("boto3.resource", return_value=MagicMock(Bucket=lambda _: mock_bucket)), \
         patch.object(csv_parser, "s3", mock_s3), \
         patch.object(csv_parser, "sns_client", mock_sns):
        csv_parser.lambda_handler(event, {})

    publish_call = mock_sns.publish.call_args
    message = json.loads(publish_call.kwargs["Message"])
    assert message["retAddr"] == "user@example.com"


# ── _get_object_with_retry ────────────────────────────────────────────────────

def test_get_object_with_retry_succeeds_first_attempt():
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": b"data"}
    with patch.object(csv_parser, "s3", mock_s3):
        result = csv_parser._get_object_with_retry("bucket", "key")
    assert mock_s3.get_object.call_count == 1
    assert result == {"Body": b"data"}


def test_get_object_with_retry_raises_no_such_key_immediately():
    mock_s3 = MagicMock()
    mock_s3.exceptions.NoSuchKey = KeyError
    mock_s3.get_object.side_effect = KeyError("NoSuchKey")
    with patch.object(csv_parser, "s3", mock_s3):
        with pytest.raises(KeyError):
            csv_parser._get_object_with_retry("bucket", "missing-key")
    assert mock_s3.get_object.call_count == 1


def test_get_object_with_retry_retries_transient_errors():
    mock_s3 = MagicMock()
    mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
    mock_s3.get_object.side_effect = [
        ConnectionError("timeout"),
        {"Body": b"data"},
    ]
    with patch.object(csv_parser, "s3", mock_s3), \
         patch("time.sleep"):  # skip actual sleep
        result = csv_parser._get_object_with_retry("bucket", "key")
    assert mock_s3.get_object.call_count == 2
    assert result == {"Body": b"data"}

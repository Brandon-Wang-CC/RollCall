import io
import json
import os
import tempfile
from unittest.mock import MagicMock, patch, call

import pytest

from conftest import load_module, make_ses_emailer_event

ses_emailer = load_module("ses_emailer", "lambdas/ses-emailer/lambda_function.py")


# ── download_file_from_s3 ─────────────────────────────────────────────────────

def test_download_calls_s3_with_correct_args(tmp_path):
    local_path = str(tmp_path / "report.xlsx")
    mock_s3 = MagicMock()
    mock_s3.download_file.side_effect = lambda b, k, p: open(p, "wb").close()

    with patch.object(ses_emailer, "s3", mock_s3):
        result = ses_emailer.download_file_from_s3("my-bucket", "prefix/key.xlsx", local_path)

    mock_s3.download_file.assert_called_once_with("my-bucket", "prefix/key.xlsx", local_path)
    assert result == local_path


# ── send_email_with_attachment ────────────────────────────────────────────────

def test_send_email_uses_correct_destination(tmp_path):
    fake_file = tmp_path / "report.xlsx"
    fake_file.write_bytes(b"PK fake xlsx content")

    mock_ses = MagicMock()
    mock_ses.send_raw_email.return_value = {"MessageId": "msg-abc"}

    with patch.object(ses_emailer, "ses", mock_ses), \
         patch.dict(os.environ, {"SENDER_EMAIL": "noreply@example.com"}):
        ses_emailer.send_email_with_attachment(
            to_email="recipient@example.com",
            subject="Test Report",
            body="See attached.",
            file_path=str(fake_file),
            filename="report.xlsx",
        )

    kwargs = mock_ses.send_raw_email.call_args.kwargs
    assert kwargs["Destinations"] == ["recipient@example.com"]
    assert kwargs["Source"] == "noreply@example.com"


def test_send_email_raw_message_contains_recipient(tmp_path):
    fake_file = tmp_path / "data.xlsx"
    fake_file.write_bytes(b"PK")

    mock_ses = MagicMock()
    mock_ses.send_raw_email.return_value = {"MessageId": "msg-xyz"}

    with patch.object(ses_emailer, "ses", mock_ses), \
         patch.dict(os.environ, {"SENDER_EMAIL": "sender@example.com"}):
        ses_emailer.send_email_with_attachment(
            to_email="user@corp.com",
            subject="Pipeline Complete",
            body="Attached.",
            file_path=str(fake_file),
        )

    raw_data = mock_ses.send_raw_email.call_args.kwargs["RawMessage"]["Data"]
    assert "user@corp.com" in raw_data


# ── lambda_handler — URL decoding ─────────────────────────────────────────────

def test_lambda_handler_decodes_percent_encoded_at_sign():
    # S3 encodes @ as %40 and spaces as + in object key notifications
    captured = {}

    def fake_download(bucket, key, local_path="/tmp/report.xlsx"):
        captured["key"] = key
        return "/tmp/fake.xlsx"

    event = make_ses_emailer_event(
        key="user%40example.com/report+data.xlsx",
        bucket="test-depts",
    )

    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = {"Metadata": {}}

    with patch.object(ses_emailer, "s3", mock_s3), \
         patch.object(ses_emailer, "download_file_from_s3", side_effect=fake_download), \
         patch.object(ses_emailer, "send_email_with_attachment",
                      return_value={"MessageId": "msg-1"}), \
         patch.dict(os.environ, {"SENDER_EMAIL": "noreply@example.com"}):
        ses_emailer.lambda_handler(event, {})

    assert captured["key"] == "user@example.com/report data.xlsx"


def test_lambda_handler_extracts_recipient_from_key_prefix():
    captured = {}

    def fake_send(to_email, subject, body, file_path, filename=None, **kwargs):
        captured["to_email"] = to_email
        return {"MessageId": "msg-2"}

    event = make_ses_emailer_event(
        key="testuser%40corp.com/output.xlsx",
        bucket="test-depts",
    )

    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = {"Metadata": {}}

    with patch.object(ses_emailer, "s3", mock_s3), \
         patch.object(ses_emailer, "download_file_from_s3", return_value="/tmp/fake.xlsx"), \
         patch.object(ses_emailer, "send_email_with_attachment", side_effect=fake_send), \
         patch.dict(os.environ, {"SENDER_EMAIL": "noreply@example.com"}):
        ses_emailer.lambda_handler(event, {})

    assert captured["to_email"] == "testuser@corp.com"


# ── lambda_handler — key without slash ───────────────────────────────────────

def test_lambda_handler_skips_key_without_slash():
    event = make_ses_emailer_event(key="flat-file-no-email-prefix.xlsx", bucket="test-depts")

    with patch.object(ses_emailer, "download_file_from_s3") as mock_dl:
        result = ses_emailer.lambda_handler(event, {})

    mock_dl.assert_not_called()
    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert "skipped" in body


# ── lambda_handler — happy path ───────────────────────────────────────────────

def test_lambda_handler_happy_path_returns_200_with_message_id():
    event = make_ses_emailer_event(
        key="user%40example.com/data.xlsx",
        bucket="test-depts",
    )

    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = {"Metadata": {}}

    with patch.object(ses_emailer, "s3", mock_s3), \
         patch.object(ses_emailer, "download_file_from_s3", return_value="/tmp/fake.xlsx"), \
         patch.object(ses_emailer, "send_email_with_attachment",
                      return_value={"MessageId": "ses-msg-xyz"}), \
         patch.dict(os.environ, {"SENDER_EMAIL": "noreply@example.com"}):
        result = ses_emailer.lambda_handler(event, {})

    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["sesMessageId"] == "ses-msg-xyz"
    assert body["to"] == "user@example.com"


def test_lambda_handler_passes_bucket_and_decoded_key_to_download():
    captured = {}

    def fake_download(bucket, key, local_path="/tmp/report.xlsx"):
        captured["bucket"] = bucket
        captured["key"] = key
        return "/tmp/fake.xlsx"

    event = make_ses_emailer_event(
        key="user%40example.com/report.xlsx",
        bucket="prod-depts-bucket",
    )

    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = {"Metadata": {}}

    with patch.object(ses_emailer, "s3", mock_s3), \
         patch.object(ses_emailer, "download_file_from_s3", side_effect=fake_download), \
         patch.object(ses_emailer, "send_email_with_attachment",
                      return_value={"MessageId": "msg-3"}), \
         patch.dict(os.environ, {"SENDER_EMAIL": "noreply@example.com"}):
        ses_emailer.lambda_handler(event, {})

    assert captured["bucket"] == "prod-depts-bucket"
    assert captured["key"] == "user@example.com/report.xlsx"


# ── lambda_handler — error propagation ───────────────────────────────────────

def test_lambda_handler_propagates_s3_download_error():
    event = make_ses_emailer_event(
        key="user%40example.com/data.xlsx",
        bucket="test-depts",
    )

    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = {"Metadata": {}}

    with patch.object(ses_emailer, "s3", mock_s3), \
         patch.object(ses_emailer, "download_file_from_s3",
                      side_effect=Exception("S3 access denied")):
        with pytest.raises(Exception, match="S3 access denied"):
            ses_emailer.lambda_handler(event, {})


def test_lambda_handler_propagates_ses_send_error():
    event = make_ses_emailer_event(
        key="user%40example.com/data.xlsx",
        bucket="test-depts",
    )

    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = {"Metadata": {}}

    with patch.object(ses_emailer, "s3", mock_s3), \
         patch.object(ses_emailer, "download_file_from_s3", return_value="/tmp/fake.xlsx"), \
         patch.object(ses_emailer, "send_email_with_attachment",
                      side_effect=Exception("SES: Address not verified")):
        with pytest.raises(Exception, match="SES: Address not verified"):
            ses_emailer.lambda_handler(event, {})

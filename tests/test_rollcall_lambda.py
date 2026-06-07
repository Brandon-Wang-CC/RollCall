import io
import json
import os
import tempfile
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import openpyxl
import pytest

from conftest import load_module, make_xlsx_bytes, make_rollcall_event

rollcall = load_module("rollcall", "lambdas/rollcall-lambda/lambda_function.py")


# ── Fixtures ──────────────────────────────────────────────────────────────────

def make_ref(req_nums=None, cc_map=None, dept_map=None):
    """Minimal reference data for build_* functions."""
    req_nums = req_nums or []
    cc_map = cc_map or {1001: "Test Department"}
    dept_map = dept_map or {"Test Department": "Test Division"}

    _ESF_COLS = ["Req #", "Job Code", "Job Profile", "Comment", "Hire Name", "Start Date"]
    esf_reqs = pd.DataFrame(
        [{"Req #": r, "Job Code": f"JC{r}", "Job Profile": "Engineer",
          "Comment": "", "Hire Name": "", "Start Date": ""}
         for r in req_nums],
        columns=_ESF_COLS,
    )
    cc_id = pd.DataFrame(
        [{"cc_id": k, "subdepartment": v} for k, v in cc_map.items()]
    )
    depts = pd.DataFrame(
        [{"department": k, "MD-2": v} for k, v in dept_map.items()]
    )
    status = pd.DataFrame([
        {"status": "Offer",                "short status": "ST1"},
        {"status": "Ready for Hire",       "short status": "ST2"},
        {"status": "Employment Agreement", "short status": "ST3"},
        {"status": "Background Check",     "short status": "ST4"},
    ])
    esf_all = pd.DataFrame(columns=["Manager Name", "Department"])
    return {"cc_id": cc_id, "depts": depts, "status": status,
            "esf_reqs": esf_reqs, "esf_all": esf_all}


# ── get_newest_file_by_prefix ─────────────────────────────────────────────────

def test_get_newest_file_by_prefix_returns_most_recent():
    older = datetime(2024, 1, 1, tzinfo=timezone.utc)
    newer = datetime(2024, 6, 1, tzinfo=timezone.utc)
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {"Contents": [
        {"Key": "Unfilled-old.xlsx", "LastModified": older},
        {"Key": "Unfilled-new.xlsx", "LastModified": newer},
        {"Key": "Other-file.xlsx",   "LastModified": newer},
    ]}
    with patch.object(rollcall, "s3", mock_s3):
        result = rollcall.get_newest_file_by_prefix("bucket", "Unfilled")
    assert result == "Unfilled-new.xlsx"


def test_get_newest_file_by_prefix_ignores_non_xlsx():
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {"Contents": [
        {"Key": "Unfilled-report.csv", "LastModified": ts},
    ]}
    with patch.object(rollcall, "s3", mock_s3):
        with pytest.raises(FileNotFoundError):
            rollcall.get_newest_file_by_prefix("bucket", "Unfilled")


def test_get_newest_file_by_prefix_raises_when_empty():
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {"Contents": []}
    with patch.object(rollcall, "s3", mock_s3):
        with pytest.raises(FileNotFoundError, match="Unfilled"):
            rollcall.get_newest_file_by_prefix("bucket", "Unfilled")


# ── find_header_row ───────────────────────────────────────────────────────────

def test_find_header_row_detects_correct_row():
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
        path = f.name
    try:
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["Junk row", "ignored"])
        ws.append(["Another junk row"])
        ws.append(["Subdivision", "Requisition Number", "Cost Center ID"])
        wb.save(path)
        row = rollcall.find_header_row(path, ["Subdivision", "Requisition Number"], sheet_name=0)
    finally:
        os.unlink(path)
    # Header is in row index 2 (0-based), so find_header_row returns 3
    assert row == 3


def test_find_header_row_raises_when_not_found():
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
        path = f.name
    try:
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["Col A", "Col B"])
        wb.save(path)
        with pytest.raises(ValueError, match="header row"):
            rollcall.find_header_row(path, ["Missing Column"], sheet_name=0)
    finally:
        os.unlink(path)


# ── build_crew_unfilled ───────────────────────────────────────────────────────

def test_build_crew_unfilled_new_req():
    unfilled = pd.DataFrame([{
        "Requisition Number": 1234567,
        "Cost Center ID": 1001,
        "Grade Grouping - GTA": "P4",
        "Hiring Manager Name": "Alice",
        "Job Requisition Status": "Open",
        "Number of Openings Total": 1,
        "ID": "JC001",
        "Job Profile Name": "Engineer",
        "State": "NY",
        "Job Requisition Primary Location (Building)": "HQ",
        "Job Requisition Additional Locations": "",
    }])
    candidates = pd.DataFrame(columns=[
        "Job Requisition", "Candidate Name", "Candidate Status", "Cost Center"
    ])
    filtered = {"unfilled": unfilled, "candidates": candidates}
    ref = make_ref(req_nums=[])  # req 1234567 is NOT in esf_reqs → "NEW"

    result = rollcall.build_crew_unfilled(filtered, ref)

    assert len(result) == 1
    assert result.iloc[0]["Existing v New"] == "NEW"
    assert result.iloc[0]["Department"] == "Test Department"
    assert result.iloc[0]["MD-2"] == "Test Division"
    assert result.iloc[0]["Management Type"] == "Non Management"


def test_build_crew_unfilled_existing_req():
    unfilled = pd.DataFrame([{
        "Requisition Number": 9999999,
        "Cost Center ID": 1001,
        "Grade Grouping - GTA": "M2",
        "Hiring Manager Name": "Bob",
        "Job Requisition Status": "Open",
        "Number of Openings Total": 1,
        "ID": "JC999",
        "Job Profile Name": "Director",
        "State": "NY",
        "Job Requisition Primary Location (Building)": "",
        "Job Requisition Additional Locations": "",
    }])
    candidates = pd.DataFrame(columns=[
        "Job Requisition", "Candidate Name", "Candidate Status", "Cost Center"
    ])
    filtered = {"unfilled": unfilled, "candidates": candidates}
    ref = make_ref(req_nums=[9999999])  # req IS in esf_reqs → "Existing"

    result = rollcall.build_crew_unfilled(filtered, ref)

    assert result.iloc[0]["Existing v New"] == "Existing"
    assert result.iloc[0]["Management Type"] == "Management"


def test_build_crew_unfilled_hire_name_lookup():
    unfilled = pd.DataFrame([{
        "Requisition Number": 1234567,
        "Cost Center ID": 1001,
        "Grade Grouping - GTA": "P3",
        "Hiring Manager Name": "Alice",
        "Job Requisition Status": "Open",
        "Number of Openings Total": 1,
        "ID": "JC001",
        "Job Profile Name": "Engineer",
        "State": "NY",
        "Job Requisition Primary Location (Building)": "",
        "Job Requisition Additional Locations": "",
    }])
    candidates = pd.DataFrame([{
        "Job Requisition": "1234567-R01",
        "Candidate Name": "Jane Smith",
        "Candidate Status": "Ready for Hire",
        "Cost Center": "1001",
    }])
    filtered = {"unfilled": unfilled, "candidates": candidates}
    ref = make_ref()

    result = rollcall.build_crew_unfilled(filtered, ref)
    assert result.iloc[0]["Hire Name"] == "Jane Smith"


def test_build_crew_unfilled_skips_nan_req_number():
    import numpy as np
    unfilled = pd.DataFrame([{"Requisition Number": float("nan"), "Cost Center ID": 1001}])
    candidates = pd.DataFrame(columns=["Job Requisition", "Candidate Name", "Candidate Status"])
    filtered = {"unfilled": unfilled, "candidates": candidates}
    ref = make_ref()
    result = rollcall.build_crew_unfilled(filtered, ref)
    assert len(result) == 0


# ── build_crew_filled ─────────────────────────────────────────────────────────

def test_build_crew_filled_filters_by_status_and_date():
    today = pd.Timestamp.today()
    candidates = pd.DataFrame([
        # Should be included: req in esf_reqs, right status, recent start date
        {"Job Requisition": "1234567-R01", "Candidate Name": "Alice",
         "Candidate Status": "Offer", "Candidate Start Date": today,
         "Cost Center": "1001", "Grade": "P3",
         "Hiring Manager": "Bob", "State": "NY",
         "Job Requisition Primary Location": "HQ"},
        # Should be excluded: status not in target list
        {"Job Requisition": "1234567-R01", "Candidate Name": "Excluded",
         "Candidate Status": "Rejected", "Candidate Start Date": today,
         "Cost Center": "1001", "Grade": "P3",
         "Hiring Manager": "Bob", "State": "NY",
         "Job Requisition Primary Location": "HQ"},
    ])
    filtered = {"candidates": candidates}
    ref = make_ref(req_nums=[1234567])
    ref["esf_reqs"]["Req #"] = pd.to_numeric(ref["esf_reqs"]["Req #"], errors="coerce")

    result = rollcall.build_crew_filled(filtered, ref)
    assert len(result) == 1
    assert result.iloc[0]["Hire Name"] == "Alice"


# ── write_output_workbook — merge logic ───────────────────────────────────────

def test_write_output_workbook_carries_forward_old_rows(tmp_path):
    # Previous run had req 111 and req 222. New run only has req 222.
    # Req 111 should be carried forward with "Carried Forward" label.
    prev_df = pd.DataFrame([
        {"Req #": "111", "Existing v New": "Existing", "Department": "OldDept"},
        {"Req #": "222", "Existing v New": "Existing", "Department": "NewDept"},
    ])
    for col in rollcall.MASTER_COLUMNS:
        if col not in prev_df.columns:
            prev_df[col] = ""

    # Serialize previous run to xlsx bytes
    prev_buf = io.BytesIO()
    with pd.ExcelWriter(prev_buf, engine="openpyxl") as w:
        prev_df[rollcall.MASTER_COLUMNS].to_excel(w, sheet_name="Output", index=False)
    prev_bytes = prev_buf.getvalue()

    # New data only has req 222
    new_df = pd.DataFrame([{"Req #": "222", "Department": "Updated"}])

    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": io.BytesIO(prev_bytes)}
    mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})

    empty = pd.DataFrame(columns=rollcall.MASTER_COLUMNS)

    with patch.object(rollcall, "s3", mock_s3), \
         patch.object(rollcall, "DEPTS_BUCKET", "test-depts"), \
         patch.object(rollcall, "TMP_DIR", str(tmp_path)):
        rollcall.write_output_workbook(new_df, empty, empty, empty, "sender@example.com")

    upload_args = mock_s3.upload_file.call_args
    out_path = upload_args.args[0]
    result = pd.read_excel(out_path, sheet_name="Output")

    req_111 = result[result["Req #"].astype(str) == "111"]
    req_222 = result[result["Req #"].astype(str) == "222"]
    assert len(req_111) == 1
    assert req_111.iloc[0]["Existing v New"] == "Carried Forward"
    assert len(req_222) == 1


def test_write_output_workbook_fresh_start_when_no_previous(tmp_path):
    NoSuchKey = type("NoSuchKey", (Exception,), {})
    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = NoSuchKey()
    mock_s3.exceptions.NoSuchKey = NoSuchKey

    new_df = pd.DataFrame([{"Req #": "999"}])
    empty = pd.DataFrame(columns=rollcall.MASTER_COLUMNS)

    with patch.object(rollcall, "s3", mock_s3), \
         patch.object(rollcall, "DEPTS_BUCKET", "test-depts"), \
         patch.object(rollcall, "TMP_DIR", str(tmp_path)):
        rollcall.write_output_workbook(new_df, empty, empty, empty, "sender@example.com")

    out_path = mock_s3.upload_file.call_args.args[0]
    result = pd.read_excel(out_path, sheet_name="Output")
    assert len(result) == 1


# ── lambda_handler ────────────────────────────────────────────────────────────

def test_lambda_handler_extracts_ret_addr():
    captured = {}

    def fake_discover(bucket):
        captured["called"] = True
        raise RuntimeError("stop here")

    event = make_rollcall_event(ret_addr="user@example.com")
    with patch.object(rollcall, "discover_files", side_effect=fake_discover), \
         patch.object(rollcall, "s3", MagicMock()):
        with pytest.raises(RuntimeError):
            rollcall.lambda_handler(event, {})

    assert captured["called"]


def test_lambda_handler_reraises_on_error():
    event = make_rollcall_event()
    with patch.object(rollcall, "discover_files", side_effect=FileNotFoundError("missing")), \
         patch.object(rollcall, "s3", MagicMock()):
        with pytest.raises(FileNotFoundError):
            rollcall.lambda_handler(event, {})

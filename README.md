# RollCall

## Project Overview

RollCall is an AWS-based automation system that processes Workday workforce reports and generates a consolidated workforce tracking file.

The goal of the project is to reduce manual effort by automatically collecting, filtering, and organizing workforce data into a standardized Excel report to be used for ES&F Vanguard Tableau dashboards. 

---

## System Workflow

1. Workday reports are received through email.
2. The reports are uploaded to Amazon S3.
3. The **CsvParser** Lambda processes the reports and saves the results back to S3.
4. An S3 Event Notification sends a message to an SQS queue.
5. The SQS queue triggers the **RollCall Lambda**.
6. The RollCall Lambda processes the reports, applies business rules, and generates the final workforce report.
7. The completed report is uploaded back to Amazon S3.

---

## Features Implemented

- Automated processing of Workday reports.
- Automatic retrieval of report files from Amazon S3.
- Filtering and processing of workforce data.
- Processing of:
  - **Crew Unfilled Requisitions**
  - **Crew Filled Requisitions**
  - **Contractor Unfilled Requisitions**
  - **Contractor Filled Requisitions**
- Use of reference data to enrich report information.
- Automatic generation of a consolidated Excel workbook.
- Upload of the final report to Amazon S3.
- Support for AWS Lambda deployment.

---

## Features Not Implemented

- Direct, automated publishing to the Tableau Server environment is not supported.
  
- The system does **not integrate with core finance systems** or perform any payroll cost evaluations.

- Data pipelines purposefully **exclude tracking profiles for Vanguard interns or seasonal co-op students**.

- The infrastructure is bounded to a **weekly batch processing refresh cycle** and does not stream live daily updates.

- The system **does not** possess permissions to **create, update, or delete entries** within core Workday/HR sources.

---

## Known Issues

- None

---

## Workarounds

- None

---

## Help 

- If a message keeps failing during processing, the system will try 5 times. After that, it stops retrying and moves the message to an **SQS Dead-Letter Queue (DLQ)** so it doesn’t block the rest of the pipeline.

- **If new columns are added the pipeline will not break**; the transformation script ignores any columns it does not recognize. However, if the process is to be executed with completely new data with new columns, an administrator must manually update the `rollcall_lambda` ingestion logic to accommodate those changes. 

- **If existing columns are deleted or renamed the pipeline will break** because the script depends on specific required fields defined in the data dictionary. 

---

### Requirements

- Python 3.12

---

## Output

The system generates:

```text
ESF WF data file.xlsx
```

This file contains the consolidated workforce data generated from the latest Workday reports.

### Existing v New column

Each row is classified with one of four values:

| Value | Meaning |
|-------|---------|
| `New` | Req number has never appeared in any previous pipeline run |
| `Existing` | Req is known; no changes detected since the last run |
| `Updated` | Req is known but the hire name or start date has changed |
| `Removed` | Req was present in the previous run but is absent from the current reports |

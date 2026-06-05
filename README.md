# RollCall

## Project Overview

RollCall is an AWS-based automation system that processes Workday workforce reports and generates a consolidated workforce tracking file.

The goal of the project is to reduce manual effort by automatically collecting, filtering, and organizing workforce data into a standardized Excel report.

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
  - Crew Unfilled Requisitions
  - Crew Filled Requisitions
  - Contractor Unfilled Requisitions
  - Contractor Filled Requisitions
- Use of reference data to enrich report information.
- Automatic generation of a consolidated Excel workbook.
- Upload of the final report to Amazon S3.
- Support for AWS Lambda deployment.

---

## Features Not Implemented

- None

---

## Known Issues

- None

---

## Workarounds

- None

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

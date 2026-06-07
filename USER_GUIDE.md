# RollCall — User Guide

## Table of Contents

- [What RollCall Does](#what-rollcall-does)
- [Prerequisites](#prerequisites)
- [First-Time Setup](#first-time-setup)
  - [Quick-Start Checklist](#quick-start-checklist)
  - [Step 1 — Fill In Configuration Files](#step-1--fill-in-configuration-files)
  - [Step 2 — GitHub Actions AWS Access](#step-2--github-actions-aws-access-aws-team-task)
  - [Step 3 — Run the Deploy](#step-3--run-the-deploy)
  - [Step 4 — Add DNS Records](#step-4--add-dns-records-dns-team-task)
  - [Step 5 — Request SES Production Access](#step-5--request-ses-production-access-aws-support-task)
  - [Step 6 — Test the Pipeline](#step-6--test-the-pipeline)
- [Day-to-Day Operation](#day-to-day-operation)
  - [Triggering a Run](#triggering-a-run)
  - [Reading the Output Workbook](#reading-the-output-workbook)
- [Failure Notifications](#failure-notifications)
- [Carry-Forward Tracking](#carry-forward-tracking)
- [GitHub Actions Workflows](#github-actions-workflows)
  - [Deploy](#deploy)
  - [Switch SES Receipt Rule Set](#switch-ses-receipt-rule-set)
  - [Reset Seeding](#reset-seeding)
  - [Delete Stack](#delete-stack)
  - [Snapshot / Restore](#snapshot--restore)
- [Troubleshooting](#troubleshooting)

---

## What RollCall Does

RollCall is an automated headcount reconciliation pipeline. Each week, you email a set of XLSX report exports to a pipeline address. Within minutes, the pipeline emails back a single combined output workbook that:

- Merges Crew (unfilled and filled) and Contractor (unfilled and filled) data into one sheet
- Annotates each requisition as **NEW**, **Open**, **Filled**, or **Removed**
- Requisitions that disappear from the current reports are labeled "Removed" in the next output, then automatically dropped from future runs

No manual data wrangling. The same address receives the trigger email and sends the output back to you.

---

## Prerequisites

Before setting up, confirm you have the following:

**AWS:**
- An AWS account with administrator access (or an admin who can act on your behalf)
- The account must be in a region that supports SES email receiving: `us-east-1`, `us-west-2`, or `eu-west-1`

**DNS:**
- A subdomain your organization controls exclusively for the pipeline — for example, `pipeline.yourdomain.com`. This **cannot** be your root domain. The MX record required for email receiving would redirect all employee mail to AWS if set on the root domain. Your DNS or network team will need to create this subdomain and add records to it (Step 4 of setup).

**GitHub:**
- This repository forked or cloned under your GitHub organization
- GitHub Actions enabled on the repository

**Report files:**
The pipeline expects exactly three attachments on each trigger email. The filenames must begin with the prefixes configured in `user-settings.yaml`. Out of the box these are:

| Report | Filename prefix |
|--------|----------------|
| Unfilled Requisitions | `ES&F_GR&S Unfilled Requisition Report` |
| Contractor Report (Open + Closed sheets) | `NEW-IT Contractor-VG-Vendor Req Report-Open` / `-Closed` |
| Candidate Flow Report | `GR&S Candidate Flow Weekly Report` |

The Contractor report is one workbook with two sheets (Open and Closed) — it counts as one attachment. If you receive reports with different filename prefixes, update `FilePrefixUnfilled`, `FilePrefixContractorOpen`, `FilePrefixContractorClosed`, and `FilePrefixCandidates` in `user-settings.yaml` before deploying.

---

## First-Time Setup

### Quick-Start Checklist

- [ ] Fill in `user-settings.yaml`
- [ ] Fill in `.github/deploy-config.yaml`
- [ ] Set the alarm contact email in `.github/CODEOWNERS`
- [ ] Seed `reference-data/ESF WF data file ref.xlsx` with your most recent output workbook
- [ ] AWS team: create the OIDC identity provider and IAM role (Step 2)
- [ ] Run the Deploy workflow (Step 3)
- [ ] Confirm the SNS alarm subscription email
- [ ] DNS team: add DKIM CNAMEs, MX record, and SPF TXT record (Step 4)
- [ ] AWS Support: request SES production access if still in sandbox (Step 5)
- [ ] Send a test email and verify the output arrives (Step 6)

---

### Step 1 — Fill In Configuration Files

**`user-settings.yaml`** (root of repo)

| Setting | What to put here |
|---------|-----------------|
| `SenderDomain` | The pipeline subdomain — e.g., `pipeline.yourdomain.com` |
| `SenderEmail` | The full pipeline address — e.g., `rollcall@pipeline.yourdomain.com` |
| `MD1Name` | Name and employee ID stamped into the MD-1 column of every output row |
| `FilePrefixUnfilled` | Start of the unfilled requisitions filename |
| `FilePrefixContractorOpen` | Start of the contractor open report filename |
| `FilePrefixContractorClosed` | Start of the contractor closed report filename |
| `FilePrefixCandidates` | Start of the candidate flow report filename |

**`.github/deploy-config.yaml`**

| Setting | What to put here |
|---------|-----------------|
| `aws.region` | AWS region — must be `us-east-1`, `us-west-2`, or `eu-west-1` |
| `aws.oidcRoleArn` | ARN of the IAM role created by your AWS team in Step 2 |
| `buckets.csv` | Globally unique S3 bucket name for parsed attachments — e.g., `yourorg-rollcall-csv` |
| `buckets.depts` | Globally unique S3 bucket name for reference data and output workbooks — e.g., `yourorg-rollcall-data` |
| `buckets.ses` | Globally unique S3 bucket name for raw inbound emails — e.g., `yourorg-rollcall-ses` |

S3 bucket names must be globally unique across all AWS accounts. If a name is already taken, the deploy will fail with `BucketAlreadyExists`. Use names that include your organization's name to reduce collisions.

The `stacks` section lists CloudFormation stack names. These can be left as-is unless they conflict with existing stacks in your account.

**`.github/CODEOWNERS`**

Replace the placeholder email with the address that should receive CloudWatch alarm notifications. An alarm fires when **3 or more Lambda errors occur within a single 24-hour window**. This is typically an operations or on-call contact. The alarm email includes the name of the affected Lambda and the SQS dead-letter queue where failed messages are held.

**Seed the reference workbook**

The `reference-data/` folder is synced to `DeptsBucket` on every deploy. It contains lookup files the pipeline reads at runtime and one workbook, `ESF WF data file ref.xlsx`, that serves a dual purpose:

1. **Lookup data** — the `Reqs` and `ALL` sheets are read each run to determine whether requisitions are new or already known.
2. **First-run carry-forward seed** — on a fresh deploy, before the pipeline has produced any output of its own, the `Reqs` sheet of this file is used to populate carry-forward tracking. Without a current seed, all requisitions in your first run will appear as new rather than continuing from where you left off.

**Before your first deploy, replace `reference-data/ESF WF data file ref.xlsx` with your most recent output workbook.** Commit the replacement and push — the deploy workflow will upload it.

After the first pipeline run completes, the pipeline writes a live reference file to `DeptsBucket` and uses that going forward. The seed file is still re-uploaded on each subsequent deploy, but the live reference always takes precedence.

---

### Step 2 — GitHub Actions AWS Access (AWS Team Task)

The deploy workflow authenticates to AWS using short-lived OIDC tokens — no stored credentials. This is a one-time setup task for your AWS administrator.

Ask your AWS team to:

1. Create an IAM OIDC identity provider for `https://token.actions.githubusercontent.com` (if one doesn't already exist in the account)
2. Create an IAM role trusted by that provider, scoped to this repository (`repo:YOUR_ORG/YOUR_REPO:*`), with `AdministratorAccess` or equivalent permissions covering CloudFormation, Lambda, IAM, SES, SQS, SNS, and S3
3. Provide you the role ARN to paste into `.github/deploy-config.yaml`

References:
- [GitHub: Configuring OIDC with AWS](https://docs.github.com/actions/deployment/security-hardening-your-deployments/configuring-openid-connect-in-amazon-web-services)
- [AWS: Creating a role for OIDC federation](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-idp_oidc.html)

---

### Step 3 — Run the Deploy

1. Go to your GitHub repository → **Actions → Deploy**
2. Click **Run workflow** → select `master` → choose environment (`prod`) → **Run workflow**
3. The workflow deploys resources in order: IAM roles → S3 buckets → reference data → SNS → SQS → SES → Lambda functions → CloudWatch alarms. It takes several minutes.

If a step fails, open the **AWS CloudFormation console**, click the failing stack, and open the **Events** tab — the error is listed there in plain English.

**After the deploy — confirm the alarm notification subscription**

When the CloudWatch alarm SNS topics are created, AWS sends a confirmation email to the address in `CODEOWNERS`. The recipient must click **Confirm subscription** in that email before alarm notifications will be delivered. The email comes from `no-reply@sns.amazonaws.com` with subject "AWS Notification - Subscription Confirmation". Check spam if it doesn't arrive within a few minutes.

---

### Step 4 — Add DNS Records (DNS Team Task)

AWS cannot create DNS records on your behalf. Provide the following values to your DNS team after the deploy completes.

**4a — DKIM records (required for outbound email)**

Find these in the **SES console → Verified identities → your subdomain → DKIM tab**. There will be three CNAME records:

| Type | Name | Value |
|------|------|-------|
| CNAME | `<token>._domainkey.pipeline.yourdomain.com` | `<token>.dkim.amazonses.com` |
| CNAME | `<token>._domainkey.pipeline.yourdomain.com` | `<token>.dkim.amazonses.com` |
| CNAME | `<token>._domainkey.pipeline.yourdomain.com` | `<token>.dkim.amazonses.com` |

Verification completes automatically within a few hours after the records propagate.

**4b — MX record (required for inbound email)**

| Type | Name | Value | Priority |
|------|------|-------|----------|
| MX | `pipeline.yourdomain.com` | `inbound-smtp.REGION.amazonaws.com` | 10 |

Replace `REGION` with your deployment region, e.g. `inbound-smtp.us-east-1.amazonaws.com`.

**4c — SPF record (recommended)**

Prevents pipeline output emails from being marked as spam.

| Type | Name | Value |
|------|------|-------|
| TXT | `pipeline.yourdomain.com` | `"v=spf1 include:amazonses.com ~all"` |

---

### Step 5 — Request SES Production Access (AWS Support Task)

New AWS accounts can only send email to pre-verified addresses. The pipeline needs to send to arbitrary recipients, so production access must be requested.

To check: open **SES console → Account dashboard**. If no sandbox warning appears, skip this step.

If still in sandbox:
1. Open **SES console → Account dashboard → Request production access**
2. Fill in:
   - **Mail type:** Transactional
   - **Use case:** Automated internal reporting — sends processed HR workbooks to known internal recipients. Low volume (single-digit emails per run).
3. Submit — AWS typically responds within 24 hours.

**Testing while still in sandbox:** You can verify individual email addresses in **SES console → Verified identities → Create identity → Email address**. Once verified, those addresses can receive pipeline output. Each tester must be verified individually until production access is granted.

---

### Step 6 — Test the Pipeline

Once DNS records have propagated (allow up to 24–48 hours) and SES is out of sandbox:

1. Send an email to your `SenderEmail` address with all three report attachments
2. Within a few minutes, the output workbook should arrive in your inbox from that same address
3. If it doesn't arrive, check CloudWatch log groups: `/aws/lambda/csvParser`, `/aws/lambda/rollcall-lambda`, `/aws/lambda/ses-emailer-function`

---

## Day-to-Day Operation

### Triggering a Run

Send an email to the address in `SenderEmail` with the three report XLSX files attached:

- Unfilled Requisitions report (filename starting with the configured prefix)
- Contractor Vendor Req report (one file with Open and Closed sheets)
- Candidate Flow report

The subject line and email body do not matter. The output workbook is returned to the sender's address automatically, usually within 2–5 minutes.

### Reading the Output Workbook

The output workbook contains a single **Output** sheet with all requisitions from the current run plus any carried forward from the previous run. 

**`Existing v New` values:**

| Value | Appears on | Meaning |
|-------|-----------|---------|
| `NEW` | Crew Unfilled, Contractor Unfilled | Req number has never appeared in any previous run |
| `Existing` | Crew Unfilled, Crew Filled | Req is known; no change to hire name or start date |
| `Update` | Crew Filled | Hire name changed since the last ESF WF reference snapshot |
| `Update Date` | Crew Filled | Start date changed since the last ESF WF reference snapshot |
| `Open` | Contractor Unfilled | Req is in the ESF WF reference but no hire name recorded yet |
| `Filled` | Contractor Unfilled, Contractor Filled | Req is in the ESF WF reference and a hire name is present; all contractor closed/filled rows |
| `Removed` | All row types | Req was in the previous run's output but absent from this run's reports — surfaced once so it is not silently lost, then excluded from all future runs |

---

## Failure Notifications

If the pipeline cannot process a submission, it emails the original sender automatically with a failure notice. The email threads with the original submission (same subject line, prefixed with `RE:`) so it is easy to find in context.

The failure email states which stage failed (csvParser or rollcall-lambda) and includes the error message. Common causes:

| Failure stage | Typical cause |
|---------------|--------------|
| csvParser | A required attachment was missing or had an unrecognised filename prefix |
| rollcall-lambda | A required column was missing from a report, or a reference file in `DeptsBucket` was corrupt or absent |

If a failure email arrives, correct the issue and resubmit the same three reports. If the problem recurs or the error message is unclear, check the CloudWatch log group for the Lambda named in the failure email.

---

## Carry-Forward Tracking

Each pipeline run reads the previous run's output from `DeptsBucket`. Any requisition that was in the previous output but is absent from the current reports is included in the new output labeled `Existing v New = "Removed"`, then permanently dropped from the carry-forward seed. This means a removed requisition surfaces exactly once — visible in the output for the run where it disappeared — and does not appear again in subsequent runs.

**Fresh deploy:** If no previous output exists yet (first run after deploy), the pipeline bootstraps carry-forward state from the `Reqs` sheet of `reference-data/ESF WF data file ref.xlsx`. See the "Seed the reference workbook" section under Step 1.

**Resetting carry-forward state:** If you want the next run to start fresh (ignoring previous output), use the **Reset Seeding** workflow described below. This deletes the pipeline's live reference from S3, forcing the next run to fall back to the seed file. Use this with care — after a reset, requisitions from the deleted reference will no longer appear as Carried Forward.

---

## GitHub Actions Workflows

All operational actions are triggered manually from **GitHub → Actions**.

### Deploy

Deploys the full pipeline to the selected environment (`test` or `prod`). Run this after any configuration change or code update. Safe to re-run — the deploy is idempotent.

### Switch SES Receipt Rule Set

Toggles which environment (`test` or `prod`) receives live inbound email. Use this when promoting from test to production or temporarily routing email to a test environment.

### Reset Seeding

Deletes the pipeline's live carry-forward reference file from `DeptsBucket`. The next pipeline run will bootstrap from `reference-data/ESF WF data file ref.xlsx` instead, as if deploying fresh. Use this when:
- You want to re-seed with a newer output workbook after replacing the seed file and deploying
- Carry-forward state has accumulated stale rows you want to clear

To run: **Actions → Reset Seeding → Run workflow → select environment → Run workflow**.

### Delete Stack

Tears down a deployed environment. Does not delete S3 bucket contents — buckets must be emptied manually before the CloudFormation stack can be deleted if they contain objects.

### Snapshot / Restore

Saves and restores the contents of `DeptsBucket` for a given environment. Use Snapshot before a risky change; use Restore to roll back.

---

## Troubleshooting

**Deploy fails on the IAM stack**
The GitHub Actions OIDC role lacks sufficient permissions. Ask your AWS team to verify it has `AdministratorAccess` or equivalent permissions covering CloudFormation, Lambda, IAM, SES, SQS, SNS, and S3.

**Deploy fails with `BucketAlreadyExists`**
The bucket name in `user-settings.yaml` is already taken by another AWS account. Choose a more unique name and redeploy.

**Email sent to the pipeline address but no output arrives**
Work through the pipeline stages:
1. **SES console → Email receiving → Rule sets** — confirm `DeliverToS3` is listed as the active rule set and points to the correct `SesBucket`.
2. **CloudWatch → `/aws/lambda/csvParser`** — check for errors. A "missing required attachments" error means one of the three expected files was not found; verify filename prefixes match `user-settings.yaml`.
3. **CloudWatch → `/aws/lambda/rollcall-lambda`** — check for errors. A `FileNotFoundError` means csvParser didn't deposit files into `CsvBucket`, or the prefixes don't match.
4. **CloudWatch → `/aws/lambda/ses-emailer-function`** — check for errors. A `MessageRejected` error typically means the account is still in SES sandbox.

**Output arrives but Removed rows are missing or wrong**
If a requisition that disappeared from the reports is not showing as "Removed," the pipeline may have started fresh (no reference file existed). Check that the seed file was committed to `reference-data/` before deploying, and verify the deploy ran `aws s3 sync reference-data/` successfully. If carry-forward state is corrupted, use **Reset Seeding** and re-seed with a known-good output workbook. Note that a req already labeled "Removed" in a prior run will not reappear — this is expected behavior.

**Alarm emails are not being received**
The SNS subscription was not confirmed. Open **SNS console → Topics**, find `Lambda1_Error_Notif` or `Lambda2_Error_Notif`, click **Subscriptions**, and check the status. If it shows `PendingConfirmation`, use **Request confirmation** to resend the confirmation email.

**Inspecting a failed message from the dead-letter queue**
Failed messages are held in the SQS dead-letter queue for **14 days** before expiring. To inspect a failed message: open **SQS console**, find the DLQ named in the alarm email (e.g. `csv-parser-dlq` or `rollcall-dlq-sqs`), and use **Send and receive messages → Poll for messages**. The message body contains the original SQS payload. Cross-reference the timestamp with the relevant Lambda's CloudWatch log group to find the full error trace.

**ses-emailer fails with `MessageRejected` or `AccessDenied`**
The account is still in SES sandbox. Verify the recipient address in the SES console (see "Testing while still in sandbox" under Step 5), or complete the production access request.

**Pipeline processes the wrong rows (unexpected filter results)**
The source reports are filtered by subdivision before processing. The pipeline expects:
- Unfilled report: `Subdivision == "Chief Information Security Office"`
- Contractor report: `Sub-Division == "ES&F"`

If your organization's subdivision names differ, the filter columns and values in `lambdas/rollcall-lambda/lambda_function.py` → `FILTER_CONFIG` will need to be updated.

**A new deploy doesn't pick up the updated seed file**
The seed file in `reference-data/` is synced on every deploy, but the pipeline's live reference file (`ESF WF data file.ref` in `DeptsBucket`) takes precedence once it exists. To force the pipeline to use the new seed, run **Reset Seeding** after deploying.

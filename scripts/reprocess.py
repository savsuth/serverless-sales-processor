#!/usr/bin/env python3
"""Re-runs jobs under the Lambda's current rules (see src/file_pipeline/admin.py).

Pick jobs by ID or by status; optionally only those processed under an
older schema version. Shows what it will do and asks before changing
anything. Table and queue default to `terraform output` from infra/.

    python scripts/reprocess.py --job-id <job-id>
    python scripts/reprocess.py --status dead_lettered
    python scripts/reprocess.py --status completed --status completed_with_rejections \\
        --older-than-version 2 --dry-run

Run with the deployment's AWS profile, e.g. AWS_PROFILE=csv-pipeline.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import boto3

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from file_pipeline import admin, jobs  # noqa: E402


def terraform_output(name: str) -> str:
    return subprocess.run(
        ["terraform", f"-chdir={REPO / 'infra'}", "output", "-raw", name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    which = parser.add_mutually_exclusive_group(required=True)
    which.add_argument("--job-id", action="append", default=[], help="Job ID (repeatable)")
    which.add_argument(
        "--status",
        action="append",
        default=[],
        choices=sorted(admin.REPROCESSABLE_STATUSES),
        help="All jobs with this status (repeatable)",
    )
    parser.add_argument(
        "--older-than-version",
        type=int,
        help="Only jobs processed under a schema version below this",
    )
    parser.add_argument("--table", help="Job table name (default: terraform output)")
    parser.add_argument("--queue-url", help="Processing queue URL (default: terraform output)")
    parser.add_argument("--dry-run", action="store_true", help="List the jobs, change nothing")
    parser.add_argument("--yes", action="store_true", help="Do not ask for confirmation")
    args = parser.parse_args()

    table = boto3.resource("dynamodb").Table(args.table or terraform_output("job_table_name"))
    if args.job_id:
        found = [table.get_item(Key={"job_id": job_id}).get("Item") for job_id in args.job_id]
        missing = [j for j, item in zip(args.job_id, found, strict=True) if item is None]
        if missing:
            print(f"error: no such job: {', '.join(missing)}", file=sys.stderr)
            return 1
        selected = [item for item in found if item is not None]
    else:
        selected = [job for status in args.status for job in admin.jobs_with_status(table, status)]
    if args.older_than_version is not None:
        selected = [
            job for job in selected if admin.processed_before_version(job, args.older_than_version)
        ]

    if not selected:
        print("No matching jobs.")
        return 0
    for job in selected:
        version = job.get("schema_version", "?")
        print(f"{job['job_id']}  {job['status']:<26} v{version}  {job['source_key']}")
    running = [job for job in selected if job["status"] == jobs.STATUS_PROCESSING]
    if running:
        print(f"{len(running)} job(s) are processing right now and will be skipped.")
    if args.dry_run:
        print(f"Dry run: {len(selected)} job(s) would be reprocessed.")
        return 0
    if not args.yes and input(f"Reprocess {len(selected)} job(s)? [y/N] ").lower() != "y":
        print("Nothing changed.")
        return 1

    queue_url = args.queue_url or terraform_output("processing_queue_url")
    sqs = boto3.client("sqs")
    queued = sum(admin.request_reprocess(table, sqs, queue_url, job) for job in selected)
    print(f"Queued {queued} of {len(selected)} job(s); check them with scripts/check_job.sh.")
    return 0 if queued == len(selected) else 1


if __name__ == "__main__":
    raise SystemExit(main())

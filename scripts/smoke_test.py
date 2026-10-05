#!/usr/bin/env python3
"""End-to-end check of a deployed stack. Uploads real files and leaves
them, and their jobs, behind under smoke-test/ and uploads/.

Each run generates a CSV with content no earlier run used (otherwise it
would be caught as a duplicate), then checks:

  1. the job finishes, with reports byte-identical to the local CLI's
     output for the same bytes;
  2. manifest.json lists every output with matching SHA-256 hashes;
  3. Athena returns the same row count and revenue for the job;
  4. the same bytes uploaded again end as duplicate_content;
  5. if the portal is enabled: the token is required, an upload through
     a presigned POST is processed, its downloads work, and a signed
     email link redirects to the report.

Reads `terraform output` from infra/; run with the deployment's AWS
profile, e.g.  AWS_PROFILE=csv-pipeline python scripts/smoke_test.py
"""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import boto3

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from file_pipeline import jobs, links  # noqa: E402
from file_pipeline.processor import process_csv, summary_json_bytes  # noqa: E402

FINAL = jobs.TERMINAL_STATUSES | {jobs.STATUS_DEAD_LETTERED}
results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> bool:
    results.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name}{f'  ({detail})' if detail else ''}")
    return ok


def outputs() -> dict[str, object]:
    raw = subprocess.run(
        ["terraform", f"-chdir={REPO / 'infra'}", "output", "-json"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return {name: value["value"] for name, value in json.loads(raw).items()}


def sample_csv(run_id: str) -> bytes:
    return (
        "date,product,quantity,unit_price\n"
        f"2024-01-05,Smoke {run_id},3,9.99\n"
        f"2024-02-06,Smoke {run_id},1,19.50\n"
        "2024-02-30,Widget,2,9.99\n"
    ).encode()


def wait_for_job(table, job_id: str, timeout: float = 120) -> dict | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        item = table.get_item(Key={"job_id": job_id}, ConsistentRead=True).get("Item")
        if item and item["status"] in FINAL:
            return item
        time.sleep(2)
    return None


def local_reports(data: bytes):
    rejected = io.StringIO(newline="")
    result = process_csv(io.BytesIO(data), rejected)
    return result, summary_json_bytes(result), rejected.getvalue().encode()


def upload(s3, bucket: str, key: str, data: bytes) -> str:
    version_id = s3.put_object(Bucket=bucket, Key=key, Body=data)["VersionId"]
    return jobs.compute_job_id(bucket, key, version_id)


def athena_totals(athena, out: dict, job_id: str) -> tuple[int, Decimal] | None:
    query_id = athena.start_query_execution(
        QueryString=f"SELECT count(*), sum(revenue) FROM sales WHERE job_id = '{job_id}'",
        QueryExecutionContext={"Database": out["athena_database_name"]},
        WorkGroup=out["athena_workgroup_name"],
    )["QueryExecutionId"]
    while True:
        state = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        if state["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if state["State"] != "SUCCEEDED":
        print(f"      Athena: {state.get('StateChangeReason', state['State'])}")
        return None
    row = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"][1]["Data"]
    return int(row[0]["VarCharValue"]), Decimal(row[1]["VarCharValue"])


def http(
    method: str,
    url: str,
    *,
    token: str = "",
    body: bytes | None = None,
    headers: dict | None = None,
):
    request = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    if token:
        request.add_header("Authorization", f"Bearer {token}")

    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None

    opener = urllib.request.build_opener(NoRedirect)
    try:
        with opener.open(request, timeout=30) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers), error.read()


def multipart(fields: dict[str, str], filename: str, data: bytes) -> tuple[bytes, str]:
    boundary = uuid.uuid4().hex
    parts = [
        f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'.encode()
        for k, v in fields.items()
    ]
    parts.append(
        f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{filename}"\r\n'
        "Content-Type: text/csv\r\n\r\n".encode()
        + data
        + f"\r\n--{boundary}--\r\n".encode()
    )
    return b"".join(parts), f"multipart/form-data; boundary={boundary}"


def portal_checks(out: dict, s3, table, lambda_client, run_id: str) -> None:
    base, token = str(out["portal_url"]), str(out["upload_token"])
    status, _, _ = http("POST", f"{base}/uploads", body=b'{"filename": "a.csv"}')
    check("portal refuses a request without the token", status == 401, f"HTTP {status}")

    status, _, body = http(
        "POST",
        f"{base}/uploads",
        token=token,
        body=json.dumps({"filename": f"portal-{run_id}.csv"}).encode(),
        headers={"content-type": "application/json"},
    )
    if not check("portal issues an upload form", status == 200, f"HTTP {status}"):
        return
    form = json.loads(body)
    data = sample_csv(run_id + "-portal")
    payload, content_type = multipart(form["fields"], f"portal-{run_id}.csv", data)
    status, headers, _ = http(
        "POST", form["url"], body=payload, headers={"content-type": content_type}
    )
    version_id = {k.lower(): v for k, v in headers.items()}.get("x-amz-version-id", "")
    if not check(
        "browser-style upload to S3 succeeds",
        status in (200, 204) and bool(version_id),
        f"HTTP {status}",
    ):
        return

    query = urllib.parse.urlencode({"key": form["key"], "version": version_id})
    _, _, body = http("GET", f"{base}/jobs?{query}", token=token)
    job_id = json.loads(body).get("job_id", "")
    job = wait_for_job(table, job_id) if job_id else None
    check(
        "portal upload is processed",
        bool(job) and job["status"] == "completed",
        job["status"] if job else "timed out",
    )
    if not job:
        return

    status, _, body = http("GET", f"{base}/jobs/{job_id}", token=token)
    downloads = json.loads(body).get("downloads", {})
    summary_url = downloads.get("summary.json", "")
    got = urllib.request.urlopen(summary_url, timeout=30).read() if summary_url else b""
    check("portal download link returns the report", got == local_reports(data)[1])

    env = lambda_client.get_function_configuration(FunctionName=str(out["lambda_function_name"]))[
        "Environment"
    ]["Variables"]
    signer = links.ReportLinks(base, env["LINK_SIGNING_KEY"].encode(), 3600)
    email_link = signer.for_job(job_id, now=time.time())[0]["summary.json"]
    status, headers, _ = http("GET", email_link)
    location = {k.lower(): v for k, v in headers.items()}.get("location", "")
    check(
        "signed email link redirects to the report",
        status == 302 and bool(location),
        f"HTTP {status}",
    )
    tampered = email_link[:-4] + ("0000" if not email_link.endswith("0000") else "1111")
    status, _, _ = http("GET", tampered)
    check("tampered email link is refused", status == 403, f"HTTP {status}")


def main() -> int:
    out = outputs()
    s3 = boto3.client("s3")
    table = boto3.resource("dynamodb").Table(str(out["job_table_name"]))
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    data = sample_csv(run_id)
    input_bucket, output_bucket = str(out["input_bucket_name"]), str(out["output_bucket_name"])
    print(f"Run {run_id} against {input_bucket}")

    job_id = upload(s3, input_bucket, f"smoke-test/{run_id}/sales.csv", data)
    job = wait_for_job(table, job_id)
    if not check("job finishes", bool(job), job["status"] if job else "timed out after 120 s"):
        return 1
    result, summary, rejected = local_reports(data)
    check("job status matches the local CLI", job["status"] == result.status, job["status"])

    def fetch(key: str) -> bytes:
        return s3.get_object(Bucket=output_bucket, Key=key)["Body"].read()

    check(
        "summary.json is byte-identical to local",
        fetch(f"reports/{job_id}/summary.json") == summary,
    )
    check(
        "rejected_rows.csv is byte-identical to local",
        fetch(f"reports/{job_id}/rejected_rows.csv") == rejected,
    )
    manifest = json.loads(fetch(f"reports/{job_id}/manifest.json"))
    entries = manifest["files"] + manifest["curated_files"]
    check(
        "manifest.json hashes match every output",
        all(hashlib.sha256(fetch(e["key"])).hexdigest() == e["sha256"] for e in entries),
        f"{len(entries)} files",
    )

    if out.get("athena_workgroup_name"):
        totals = athena_totals(boto3.client("athena"), out, job_id)
        check(
            "Athena agrees with the report",
            totals == (result.valid_row_count, result.total_revenue),
            str(totals),
        )

    duplicate_id = upload(s3, input_bucket, f"smoke-test/{run_id}/sales-copy.csv", data)
    duplicate = wait_for_job(table, duplicate_id)
    check(
        "re-upload is caught as a duplicate",
        bool(duplicate)
        and duplicate["status"] == "duplicate_content"
        and duplicate.get("duplicate_of") == job_id,
        duplicate["status"] if duplicate else "timed out",
    )

    if out.get("portal_url"):
        portal_checks(out, s3, table, boto3.client("lambda"), run_id)

    failed = [name for name, ok, _ in results if not ok]
    print(f"\n{len(results) - len(failed)} of {len(results)} checks passed.")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

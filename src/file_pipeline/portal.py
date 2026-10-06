"""HTTP API behind API Gateway for the upload page (tools/upload.html)
and the report links in notification emails.

Routes (API Gateway HTTP API, payload format 2.0):

    POST /uploads              Bearer token. Body {"filename": "..."}.
                               Returns a presigned POST for one new key
                               under uploads/ in the input bucket, at most
                               max_input_bytes, valid 15 minutes. The
                               browser uploads straight to S3.
    GET  /jobs/{job_id}        Bearer token. The job's status, counts,
                               errors and warnings, plus fresh 5-minute
                               download links once it has reports.
    GET  /jobs?key=&version=   Bearer token. The same, for the job of an
                               input-bucket object version (the upload
                               page knows the key and version it got back
                               from S3, not the job ID).
    GET  /r/{job_id}/{file}    No token: the HMAC signature in the query
                               string (see links.py) is the credential.
                               Redirects to a fresh 5-minute download.

The token is never stored: the function holds only its SHA-256 and
compares in constant time. Nothing here can read or change anything
except creating new upload keys, reading job records, and reading report
files.
"""

from __future__ import annotations

import base64
import functools
import hashlib
import hmac
import json
import os
import re
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import PurePosixPath
from typing import Any

from file_pipeline import jobs, links, storage
from file_pipeline.processor import MAX_INPUT_BYTES

UPLOAD_PREFIX = "uploads"
UPLOAD_URL_SECONDS = 15 * 60
DOWNLOAD_URL_SECONDS = 5 * 60

_JOB_ID_RE = re.compile(r"^[0-9a-f]{64}$")
_UNSAFE_FILENAME_CHARS = re.compile(r"[^A-Za-z0-9._ -]")


@dataclass
class PortalConfig:
    s3: Any
    table: Any
    input_bucket: str
    output_bucket: str
    upload_token_sha256: str
    link_key: bytes
    max_input_bytes: int = MAX_INPUT_BYTES
    now: Callable[[], float] = field(default=time.time)


@functools.cache
def _default_config() -> PortalConfig:
    """Built once per Lambda container and reused by warm requests."""
    import boto3

    return PortalConfig(
        s3=boto3.client("s3"),
        table=boto3.resource("dynamodb").Table(os.environ["JOB_TABLE_NAME"]),
        input_bucket=os.environ["INPUT_BUCKET_NAME"],
        output_bucket=os.environ["OUTPUT_BUCKET_NAME"],
        upload_token_sha256=os.environ["UPLOAD_TOKEN_SHA256"],
        link_key=os.environ["LINK_SIGNING_KEY"].encode(),
        max_input_bytes=int(os.environ.get("MAX_INPUT_BYTES", MAX_INPUT_BYTES)),
    )


def lambda_handler(event: dict, context: Any) -> dict:
    return handle_request(event, _default_config())


def handle_request(event: dict, config: PortalConfig) -> dict:
    method = event.get("requestContext", {}).get("http", {}).get("method", "")
    parts = [p for p in event.get("rawPath", "").split("/") if p]

    if method == "POST" and parts == ["uploads"]:
        return _authorized(event, config) or _create_upload(event, config)
    if method == "GET" and len(parts) == 2 and parts[0] == "jobs":
        return _authorized(event, config) or _get_job(parts[1], config)
    if method == "GET" and parts == ["jobs"]:
        return _authorized(event, config) or _get_job_of_upload(event, config)
    if method == "GET" and len(parts) == 3 and parts[0] == "r":
        return _redirect_to_report(parts[1], parts[2], event, config)
    return _json(404, {"error": "not_found"})


def _json(status: int, body: dict[str, Any]) -> dict:
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json", "cache-control": "no-store"},
        "body": json.dumps(body),
    }


def _authorized(event: dict, config: PortalConfig) -> dict | None:
    """None if the request carries the upload token, else a 401 response."""
    header = (event.get("headers") or {}).get("authorization", "")
    token = header.removeprefix("Bearer ").strip() if header.startswith("Bearer ") else ""
    digest = hashlib.sha256(token.encode()).hexdigest()
    if token and hmac.compare_digest(digest, config.upload_token_sha256):
        return None
    return _json(401, {"error": "unauthorized"})


def _request_body(event: dict) -> dict[str, Any]:
    raw = event.get("body") or ""
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode("utf-8")
    try:
        body = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return body if isinstance(body, dict) else {}


def safe_filename(filename: str) -> str:
    """Keeps only the last path part and plain characters, max 100."""
    name = PurePosixPath(filename.replace("\\", "/")).name
    return _UNSAFE_FILENAME_CHARS.sub("_", name)[-100:]


def _create_upload(event: dict, config: PortalConfig) -> dict:
    filename = safe_filename(str(_request_body(event).get("filename", "")))
    if not storage.is_input_key(filename):
        return _json(400, {"error": "The file name must end in .csv or .csv.gz."})

    date = datetime.fromtimestamp(config.now(), UTC).strftime("%Y-%m-%d")
    key = f"{UPLOAD_PREFIX}/{date}/{uuid.uuid4().hex}/{filename}"
    post = config.s3.generate_presigned_post(
        Bucket=config.input_bucket,
        Key=key,
        Conditions=[["content-length-range", 1, config.max_input_bytes]],
        ExpiresIn=UPLOAD_URL_SECONDS,
    )
    return _json(
        200,
        {
            "url": post["url"],
            "fields": post["fields"],
            "bucket": config.input_bucket,
            "key": key,
            "max_bytes": config.max_input_bytes,
            "expires_in": UPLOAD_URL_SECONDS,
        },
    )


def _plain(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else str(value)
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    return value


def _download_url(config: PortalConfig, key: str) -> str:
    return config.s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": config.output_bucket, "Key": key},
        ExpiresIn=DOWNLOAD_URL_SECONDS,
    )


def _get_job(job_id: str, config: PortalConfig) -> dict:
    if not _JOB_ID_RE.match(job_id):
        return _json(400, {"error": "Not a job ID."})
    record = config.table.get_item(Key={"job_id": job_id}).get("Item")
    if record is None:
        # The upload's event may not have reached the processor yet.
        return _json(404, {"job_id": job_id, "status": "not_found"})

    fields = (
        "status",
        "source_key",
        "valid_row_count",
        "rejected_row_count",
        "error_code",
        "error_message",
        "duplicate_of",
        "warning_counts",
        "updated_at",
    )
    body: dict[str, Any] = {"job_id": job_id}
    body.update({name: _plain(record[name]) for name in fields if name in record})
    if record.get("output_summary_key"):
        body["downloads"] = {
            filename: _download_url(config, f"reports/{job_id}/{filename}")
            for filename in links.REPORT_FILES
        }
    return _json(200, body)


def _get_job_of_upload(event: dict, config: PortalConfig) -> dict:
    query = event.get("queryStringParameters") or {}
    key, version_id = query.get("key", ""), query.get("version", "")
    if not key or not version_id:
        return _json(400, {"error": "Give the uploaded object's key and version."})
    return _get_job(jobs.compute_job_id(config.input_bucket, key, version_id), config)


def _redirect_to_report(job_id: str, filename: str, event: dict, config: PortalConfig) -> dict:
    query = event.get("queryStringParameters") or {}
    valid = _JOB_ID_RE.match(job_id) and links.verify(
        config.link_key,
        job_id,
        filename,
        query.get("exp", ""),
        query.get("sig", ""),
        now=config.now(),
    )
    if not valid:
        return _json(403, {"error": "This link has expired or is not valid."})
    return {
        "statusCode": 302,
        "headers": {
            "location": _download_url(config, f"reports/{job_id}/{filename}"),
            "cache-control": "no-store",
        },
        "body": "",
    }

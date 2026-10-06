"""The upload/report portal (src/file_pipeline/portal.py) and signed email
links (links.py), against moto."""

import base64
import hashlib
import json
import re
import urllib.parse

import boto3
import pytest
from moto import mock_aws

from file_pipeline import handler, jobs, links, portal, storage

REGION = "us-east-1"
INPUT_BUCKET = "portal-input"
OUTPUT_BUCKET = "portal-output"
TOKEN = "correct-horse-battery-staple-0123456789"
LINK_KEY = b"link-signing-key-for-tests"
CSV = b"date,product,quantity,unit_price\n2024-01-05,Widget,3,9.99\n"
NOW = 1_800_000_000.0


class FakeNotifier:
    def __init__(self):
        self.calls = []

    def publish(self, *, job_id, subject, body):
        self.calls.append((job_id, subject, body))


@pytest.fixture
def stack():
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=INPUT_BUCKET)
        s3.put_bucket_versioning(Bucket=INPUT_BUCKET, VersioningConfiguration={"Status": "Enabled"})
        s3.create_bucket(Bucket=OUTPUT_BUCKET)
        table = boto3.resource("dynamodb", region_name=REGION).create_table(
            TableName="portal-jobs",
            KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "job_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        config = portal.PortalConfig(
            s3=s3,
            table=table,
            input_bucket=INPUT_BUCKET,
            output_bucket=OUTPUT_BUCKET,
            upload_token_sha256=hashlib.sha256(TOKEN.encode()).hexdigest(),
            link_key=LINK_KEY,
            max_input_bytes=1024,
            now=lambda: NOW,
        )
        notifier = FakeNotifier()
        deps = handler.Dependencies(
            job_store=jobs.JobStore(table),
            object_storage=storage.S3Storage(s3),
            notifier=notifier,
            output_bucket=OUTPUT_BUCKET,
            report_links=links.ReportLinks(
                base_url="https://portal.example", key=LINK_KEY, valid_seconds=7 * 86400
            ),
        )
        yield {"s3": s3, "table": table, "config": config, "deps": deps, "notifier": notifier}


def request(method, path, *, token=None, body=None, query=None, base64_body=False):
    headers = {"authorization": f"Bearer {token}"} if token else {}
    raw = json.dumps(body) if body is not None else None
    if raw is not None and base64_body:
        raw = base64.b64encode(raw.encode()).decode()
    return {
        "rawPath": path,
        "requestContext": {"http": {"method": method}},
        "headers": headers,
        "body": raw,
        "isBase64Encoded": base64_body,
        "queryStringParameters": query,
    }


def process(stack, key="sales.csv"):
    version_id = stack["s3"].put_object(Bucket=INPUT_BUCKET, Key=key, Body=CSV)["VersionId"]
    body = {
        "Records": [
            {
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": INPUT_BUCKET},
                    "object": {"key": key, "versionId": version_id},
                },
            }
        ]
    }
    handler.handle_event(
        {"Records": [{"messageId": "m1", "body": json.dumps(body)}]}, stack["deps"]
    )
    return jobs.compute_job_id(INPUT_BUCKET, key, version_id)


@pytest.mark.parametrize("token", [None, "wrong", TOKEN + "x", ""])
@pytest.mark.parametrize("method,path", [("POST", "/uploads"), ("GET", "/jobs/" + "a" * 64)])
def test_token_routes_reject_a_missing_or_wrong_token(stack, token, method, path):
    response = portal.handle_request(request(method, path, token=token), stack["config"])
    assert response["statusCode"] == 401


def test_upload_returns_a_size_limited_presigned_post_for_a_fresh_key(stack):
    response = portal.handle_request(
        request("POST", "/uploads", token=TOKEN, body={"filename": "../../Q1 Sales.CSV"}),
        stack["config"],
    )
    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert re.fullmatch(r"uploads/2027-01-15/[0-9a-f]{32}/Q1 Sales\.CSV", body["key"])
    assert body["fields"]["key"] == body["key"]
    assert body["bucket"] == INPUT_BUCKET
    assert body["max_bytes"] == 1024
    policy = json.loads(base64.b64decode(body["fields"]["policy"]))
    assert ["content-length-range", 1, 1024] in policy["conditions"]


def test_upload_accepts_a_base64_encoded_body(stack):
    response = portal.handle_request(
        request("POST", "/uploads", token=TOKEN, body={"filename": "a.csv.gz"}, base64_body=True),
        stack["config"],
    )
    assert response["statusCode"] == 200


@pytest.mark.parametrize("filename", ["notes.txt", "", "sales.csv.exe", None])
def test_upload_rejects_files_that_are_not_csv(stack, filename):
    response = portal.handle_request(
        request("POST", "/uploads", token=TOKEN, body={"filename": filename}), stack["config"]
    )
    assert response["statusCode"] == 400


def test_safe_filename_keeps_only_the_last_part_and_plain_characters():
    assert portal.safe_filename("..\\..\\a/b/sales<>|.csv") == "sales___.csv"
    assert len(portal.safe_filename("x" * 500 + ".csv")) == 100


def test_job_lookup_reports_not_found_then_status_and_downloads(stack):
    missing = portal.handle_request(
        request("GET", "/jobs/" + "b" * 64, token=TOKEN), stack["config"]
    )
    assert missing["statusCode"] == 404

    job_id = process(stack)
    response = portal.handle_request(
        request("GET", f"/jobs/{job_id}", token=TOKEN), stack["config"]
    )
    body = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert (body["status"], body["valid_row_count"], body["rejected_row_count"]) == (
        "completed",
        1,
        0,
    )
    assert body["source_key"] == "sales.csv"
    assert set(body["downloads"]) == {"summary.json", "rejected_rows.csv", "manifest.json"}
    assert f"reports/{job_id}/summary.json" in body["downloads"]["summary.json"]


def test_job_lookup_rejects_something_that_is_not_a_job_id(stack):
    response = portal.handle_request(request("GET", "/jobs/../etc", token=TOKEN), stack["config"])
    assert response["statusCode"] in (400, 404)


def _email_links(stack):
    ((_, _, body),) = stack["notifier"].calls
    return dict(re.findall(r"  (\S+): (https://portal\.example\S+)", body))


def test_notification_email_links_redirect_to_the_report(stack):
    job_id = process(stack)
    email_links = _email_links(stack)
    assert set(email_links) == {"summary.json", "rejected_rows.csv"}

    url = urllib.parse.urlsplit(email_links["summary.json"])
    query = dict(urllib.parse.parse_qsl(url.query))
    stack["config"].now = lambda: float(query["exp"]) - 60  # still inside the window
    response = portal.handle_request(request("GET", url.path, query=query), stack["config"])

    assert response["statusCode"] == 302
    location = response["headers"]["location"]
    assert f"/reports/{job_id}/summary.json" in urllib.parse.urlsplit(location).path


def test_tampered_expired_or_unknown_links_are_refused(stack):
    job_id = process(stack)
    url = urllib.parse.urlsplit(_email_links(stack)["summary.json"])
    query = dict(urllib.parse.parse_qsl(url.query))
    config = stack["config"]

    def status(path, q, now):
        config.now = lambda: now
        return portal.handle_request(request("GET", path, query=q), config)["statusCode"]

    inside = float(query["exp"]) - 60
    assert status(url.path, {**query, "sig": "0" * 64}, inside) == 403
    assert status(url.path, {**query, "exp": str(int(query["exp"]) + 1)}, inside) == 403
    assert status(url.path, query, float(query["exp"]) + 1) == 403
    other = url.path.replace("summary.json", "rejected_rows.csv")
    assert status(other, query, inside) == 403  # signature is per file
    secret = f"/r/{job_id}/../../etc/passwd"
    assert status(secret, query, inside) in (403, 404)


def test_unknown_routes_are_404(stack):
    assert portal.handle_request(request("GET", "/admin"), stack["config"])["statusCode"] == 404
    assert (
        portal.handle_request(request("DELETE", "/uploads"), stack["config"])["statusCode"] == 404
    )


def test_links_verify_rejects_garbage_expiry():
    assert not links.verify(LINK_KEY, "j", "summary.json", "soon", "x", now=NOW)
    assert not links.verify(LINK_KEY, "j", "secrets.txt", str(int(NOW) + 10), "x", now=NOW)


def test_emails_have_no_download_links_when_the_portal_is_off(stack):
    stack["deps"].report_links = None
    process(stack, "plain.csv")
    ((_, _, body),) = stack["notifier"].calls
    assert "Download links" not in body
    assert "Summary report: s3://" in body


def test_job_can_be_looked_up_by_the_uploaded_key_and_version(stack):
    version_id = stack["s3"].put_object(Bucket=INPUT_BUCKET, Key="uploads/x/a.csv", Body=CSV)[
        "VersionId"
    ]
    query = {"key": "uploads/x/a.csv", "version": version_id}

    response = portal.handle_request(
        request("GET", "/jobs", token=TOKEN, query=query), stack["config"]
    )

    # Not processed yet: 404, but it already says which job to poll.
    assert response["statusCode"] == 404
    assert json.loads(response["body"])["job_id"] == jobs.compute_job_id(
        INPUT_BUCKET, "uploads/x/a.csv", version_id
    )
    missing = portal.handle_request(request("GET", "/jobs", token=TOKEN, query={}), stack["config"])
    assert missing["statusCode"] == 400

"""Signed report links for notification emails.

A link names one report file of one job and an expiry time, signed with
HMAC-SHA256 under a secret only the processor and portal Lambdas hold:

    https://<portal>/r/<job_id>/summary.json?exp=<unix time>&sig=<hex>

The portal checks the signature and expiry, then redirects to a fresh
5-minute S3 download link. An S3 presigned URL put straight into the
email would not work: one signed with a Lambda's temporary credentials
stops working when those credentials expire, often within hours,
whatever expiry it was given.
"""

from __future__ import annotations

import hashlib
import hmac
import urllib.parse
from dataclasses import dataclass

REPORT_FILES = ("summary.json", "rejected_rows.csv", "manifest.json")


def signature(key: bytes, job_id: str, filename: str, expires_at: int) -> str:
    message = f"{job_id}/{filename}/{expires_at}".encode()
    return hmac.new(key, message, hashlib.sha256).hexdigest()


def verify(
    key: bytes, job_id: str, filename: str, expires_at: str, sig: str, *, now: float
) -> bool:
    """True only for a known report file, an unexpired time, and a
    matching signature (compared in constant time)."""
    if filename not in REPORT_FILES:
        return False
    try:
        expiry = int(expires_at)
    except (TypeError, ValueError):
        return False
    if expiry < now:
        return False
    return hmac.compare_digest(signature(key, job_id, filename, expiry), sig or "")


@dataclass(frozen=True)
class ReportLinks:
    base_url: str
    key: bytes
    valid_seconds: int

    def for_job(self, job_id: str, *, now: float) -> tuple[dict[str, str], int]:
        """Links to the job's summary and rejected rows, and their expiry."""
        expires_at = int(now) + self.valid_seconds
        links = {}
        for filename in ("summary.json", "rejected_rows.csv"):
            query = urllib.parse.urlencode(
                {"exp": expires_at, "sig": signature(self.key, job_id, filename, expires_at)}
            )
            links[filename] = f"{self.base_url.rstrip('/')}/r/{job_id}/{filename}?{query}"
        return links, expires_at

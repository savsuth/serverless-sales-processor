"""S3 operations: reading the exact uploaded object version, and writing
report outputs to deterministic keys.

Report keys are deterministic by design (reports/{job_id}/...) so that a
retried job overwrites its own prior (possibly partial) output rather
than accumulating duplicates.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import IO, Any

# Matched case-insensitively, so "Sales.CSV" is processed too. S3's own
# event filter is case-sensitive, so the filtering happens here instead
# (see infra/s3.tf).
INPUT_SUFFIXES = (".csv", ".csv.gz")

SUMMARY_KEY_TEMPLATE = "reports/{job_id}/summary.json"
REJECTED_KEY_TEMPLATE = "reports/{job_id}/rejected_rows.csv"


def is_input_key(key: str) -> bool:
    return key.lower().endswith(INPUT_SUFFIXES)


def summary_key(job_id: str) -> str:
    return SUMMARY_KEY_TEMPLATE.format(job_id=job_id)


def rejected_key(job_id: str) -> str:
    return REJECTED_KEY_TEMPLATE.format(job_id=job_id)


@dataclass
class ObjectHandle:
    """A streamable handle to one exact S3 object version, plus its
    reported size (from HeadObject) so callers can enforce the size limit
    before reading the body."""

    body: IO[bytes]
    content_length: int


class S3Storage:
    def __init__(self, s3_client: Any) -> None:
        self._s3 = s3_client

    def head_object(self, bucket: str, key: str, version_id: str) -> int:
        """Returns the exact object version's content length, without
        downloading the body. Enforce size limits against this before
        calling open_object."""
        response = self._s3.head_object(Bucket=bucket, Key=key, VersionId=version_id)
        return int(response["ContentLength"])

    def open_object(self, bucket: str, key: str, version_id: str) -> ObjectHandle:
        """Opens the exact object version for streaming. Always pins
        VersionId so a later overwrite of the same key can never cause us
        to read the wrong bytes for this job."""
        response = self._s3.get_object(Bucket=bucket, Key=key, VersionId=version_id)
        return ObjectHandle(body=response["Body"], content_length=int(response["ContentLength"]))

    def put_report(
        self,
        output_bucket: str,
        job_id: str,
        summary_bytes: bytes,
        rejected_csv_bytes: bytes,
        curated_files: Iterable[tuple[str, bytes]] = (),
    ) -> tuple[str, str]:
        """Writes both report objects at their deterministic keys, then
        any curated files (key, gzip bytes; see curated.py). Safe to
        call repeatedly on retry: each call fully overwrites every key, so
        a partial prior attempt (e.g. only summary.json written before a
        crash) is corrected by the next successful attempt writing all of
        them again."""
        summary_object_key = summary_key(job_id)
        rejected_object_key = rejected_key(job_id)

        self._s3.put_object(
            Bucket=output_bucket,
            Key=summary_object_key,
            Body=summary_bytes,
            ContentType="application/json",
        )
        self._s3.put_object(
            Bucket=output_bucket,
            Key=rejected_object_key,
            Body=rejected_csv_bytes,
            ContentType="text/csv",
        )
        for key, body in curated_files:
            self._s3.put_object(
                Bucket=output_bucket, Key=key, Body=body, ContentType="application/gzip"
            )
        return summary_object_key, rejected_object_key

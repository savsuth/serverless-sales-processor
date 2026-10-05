#!/usr/bin/env bash
# Downloads a completed job's summary.json and rejected_rows.csv, and
# checks them against the job's manifest.json (written last, so its
# presence means every output of the job was written).
#
# Usage: scripts/fetch_report.sh <output-bucket-name> <job-id> [destination-dir]
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <output-bucket-name> <job-id> [destination-dir]" >&2
  exit 1
fi

BUCKET="$1"
JOB_ID="$2"
DEST="${3:-.}"

mkdir -p "$DEST"
if ! aws s3 cp "s3://$BUCKET/reports/$JOB_ID/manifest.json" "$DEST/manifest.json"; then
  echo "No manifest.json yet: the job has not finished writing its outputs (or wrote none)." >&2
  exit 1
fi
aws s3 cp "s3://$BUCKET/reports/$JOB_ID/summary.json" "$DEST/summary.json"
aws s3 cp "s3://$BUCKET/reports/$JOB_ID/rejected_rows.csv" "$DEST/rejected_rows.csv"

python3 - "$DEST" <<'EOF'
import hashlib
import json
import pathlib
import sys

dest = pathlib.Path(sys.argv[1])
manifest = json.loads((dest / "manifest.json").read_text())
for entry in manifest["files"]:
    name = entry["key"].rsplit("/", 1)[-1]
    digest = hashlib.sha256((dest / name).read_bytes()).hexdigest()
    if digest != entry["sha256"]:
        sys.exit(f"{name}: does not match manifest.json (sha256 {digest})")
print(f"verified against manifest.json (status: {manifest['status']})")
EOF

echo
echo "summary.json:"
cat "$DEST/summary.json"

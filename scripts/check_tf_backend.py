#!/usr/bin/env python3
"""Refuses (exit 1) unless Terraform in infra/ is initialised with the
state file of the given backend config, so `make plan ENV=...` can never
plan one environment against another environment's state.

    python scripts/check_tf_backend.py <env> <backend-config path relative to infra/>
"""

import json
import sys
from pathlib import Path

INFRA = Path(__file__).resolve().parent.parent / "infra"


def state_key(text: str) -> str | None:
    for line in text.splitlines():
        name, _, value = line.partition("=")
        if name.strip() == "key":
            return value.strip().strip('"')
    return None


def main(env: str, backend_path: str) -> int:
    backend = INFRA / backend_path
    if not backend.exists():
        print(f"No {backend.relative_to(INFRA.parent)} for ENV={env}; see infra/envs/README.md.")
        return 1
    current = INFRA / ".terraform" / "terraform.tfstate"
    if not current.exists():
        print(f"Terraform is not initialised; run: make init ENV={env}")
        return 1
    have = json.loads(current.read_text())["backend"]["config"].get("key")
    want = state_key(backend.read_text())
    if have != want:
        print(
            f"Terraform is initialised for {have}, not ENV={env} ({want}); run: make init ENV={env}"
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(*sys.argv[1:3]))

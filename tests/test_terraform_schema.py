"""The Athena table's columns are computed in Terraform (infra/schema.tf)
from the same schema file the Lambda uses. This evaluates that
computation with `terraform console` -- offline, in a scratch copy with
no backend and no providers -- and checks it against curated.py, so the
table can never disagree with the files the Lambda writes.

Skipped when Terraform is not installed.
"""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

from file_pipeline.curated import athena_columns
from file_pipeline.schema import load_schema

REPO = Path(__file__).parent.parent
TERRAFORM = shutil.which("terraform")


@pytest.mark.skipif(TERRAFORM is None, reason="terraform is not installed")
@pytest.mark.parametrize("schema_name", ["sales"])
def test_terraform_athena_columns_match_curated_py(schema_name, tmp_path):
    infra = tmp_path / "infra"
    infra.mkdir()
    for name in ("schema.tf", "variables.tf"):
        shutil.copy(REPO / "infra" / name, infra / name)
    (tmp_path / "src").symlink_to(REPO / "src")

    subprocess.run(
        [TERRAFORM, "init", "-backend=false", "-input=false"],
        cwd=infra,
        check=True,
        capture_output=True,
    )
    console = subprocess.run(
        [TERRAFORM, "console", f"-var=schema_name={schema_name}"],
        cwd=infra,
        input="jsonencode(local.curated_columns)\n",
        check=True,
        capture_output=True,
        text=True,
    )

    terraform_columns = json.loads(json.loads(console.stdout))
    assert [(c["name"], c["type"]) for c in terraform_columns] == athena_columns(
        load_schema(schema_name)
    )

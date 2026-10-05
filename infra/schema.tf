# The deployment's schema, read from the same JSON file the Lambda
# bundles (src/file_pipeline/schemas/<schema_name>.json), so the Athena
# table's columns can never drift from what the Lambda writes. A missing
# file fails the plan with the path it looked for.

locals {
  schema = jsondecode(file("${path.module}/../src/file_pipeline/schemas/${var.schema_name}.json"))

  # Mirrors athena_columns() in src/file_pipeline/curated.py -- keep the
  # two in step: every schema column, then each measure that is not
  # itself a column, then the lineage fields.
  athena_types = {
    date    = "date"
    string  = "string"
    integer = "bigint"
    decimal = "decimal(38,18)"
  }
  schema_column_types = { for c in local.schema.columns : c.name => c.type }

  curated_columns = concat(
    [for c in local.schema.columns : { name = c.name, type = local.athena_types[c.type] }],
    [
      for m in local.schema.aggregation.measures : {
        name = m.name
        type = (
          anytrue([for col in m.multiply : local.schema_column_types[col] == "decimal"])
          ? local.athena_types.decimal
          : local.athena_types.integer
        )
      } if !contains(keys(local.schema_column_types), m.name)
    ],
    [
      { name = "job_id", type = "string" },
      { name = "source_row_number", type = "bigint" },
    ],
  )

  # Athena resources exist only when the schema asks for curated output.
  athena_enabled = var.enable_athena && try(local.schema.curated.partition_by_month, null) != null
}

# Optional: a budget ALERT only. It sends an email when spend crosses the
# threshold -- it does not stop, throttle, or cap spending in any way.
#
# This tracks the whole account's spend, not just this project's
# resources: scoping a budget to a tag requires first activating that tag
# as a "cost allocation tag" in the Billing console (a manual, one-time,
# account-level step Terraform cannot perform), which is out of scope for
# a first deployment. Activate cost allocation tags and add a cost_filter
# block here if you need per-project scoping.

resource "aws_budgets_budget" "monthly" {
  count = var.enable_budget_alert ? 1 : 0

  name         = "${var.project_name}-monthly-budget"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = var.budget_alert_email != "" ? [var.budget_alert_email] : []
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = var.budget_alert_email != "" ? [var.budget_alert_email] : []
  }
}

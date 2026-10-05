resource "aws_sns_topic" "notifications" {
  name = "${var.project_name}-notifications"
}

# AWS requires the recipient to confirm this subscription (a confirmation
# email is sent to notification_email) before any messages are delivered
# -- see README's "Reliability and Limitations" section.
resource "aws_sns_topic_subscription" "email" {
  count     = var.notification_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.notifications.arn
  protocol  = "email"
  endpoint  = var.notification_email
}

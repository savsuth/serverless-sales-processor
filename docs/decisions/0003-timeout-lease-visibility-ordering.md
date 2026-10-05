# 0003. Lambda timeout < lease < queue visibility timeout

Status: accepted (original build; recorded 2026-10-04)

## Context

Three timers interact when a worker is slow or dies:

- the Lambda timeout, after which the invocation is killed;
- the job lease ([0004](0004-expiring-lease-claims.md)), after which
  another worker may take the job over;
- the SQS visibility timeout, after which an unacknowledged message is
  delivered again.

If the lease could expire while the invocation is still legitimately
running, two workers would process the same job at once. If the
message came back before the lease expired, every redelivery after a
crash would find the job still claimed and give up.

## Decision

Default values: Lambda timeout 60 s, lease 120 s, visibility timeout
360 s. The visibility timeout is derived as 6 x the Lambda timeout, the
ratio AWS documents for SQS-triggered functions, so it cannot be set
inconsistently.

## Consequences

- A running invocation always finishes or is killed before its lease
  expires, so a live worker is never treated as crashed.
- After a crash, the redelivered message arrives after the lease has
  expired, so the retry can take the job over straight away.
- Changing `lambda_timeout_seconds` moves the visibility timeout with
  it. `job_lease_seconds` is set separately and must stay between the
  two.

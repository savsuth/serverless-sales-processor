-- What each upload contributed, newest sales first. Look up a job_id in
-- the DynamoDB job table (scripts/check_job.sh) to find its source file;
-- source_row_number in this table is the row's line in that file.
SELECT
  job_id,
  COUNT(*) AS row_count,
  MIN("date") AS first_sale,
  MAX("date") AS last_sale,
  SUM(revenue) AS revenue
FROM sales
GROUP BY job_id
ORDER BY last_sale DESC;

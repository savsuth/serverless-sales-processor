-- The ten best-selling products by revenue over the last 12 months.
-- Filtering on `month` (the partition) means Athena reads only those
-- months' files, which keeps the query fast and cheap. Product names are
-- compared case-insensitively, as in the reports.
SELECT
  min(product) AS product,
  SUM(revenue) AS revenue,
  SUM(quantity) AS quantity
FROM sales
WHERE month >= date_format(date_add('month', -11, current_date), '%Y-%m')
GROUP BY lower(product)
ORDER BY revenue DESC
LIMIT 10;

-- The ten best-selling products by revenue over the last 12 months.
-- Filtering on `month` (the partition) means Athena reads only those
-- months' files, which keeps the query fast and cheap.
SELECT
  product,
  SUM(revenue) AS revenue,
  SUM(quantity) AS quantity
FROM sales
WHERE month >= date_format(date_add('month', -11, current_date), '%Y-%m')
GROUP BY product
ORDER BY revenue DESC
LIMIT 10;

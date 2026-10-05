-- Revenue and quantity per product per month, across every completed upload.
-- Duplicate uploads and failed files never reach this table, so nothing is
-- double counted.
SELECT
  month,
  product,
  SUM(quantity) AS quantity,
  SUM(revenue) AS revenue
FROM sales
GROUP BY month, product
ORDER BY month, revenue DESC;

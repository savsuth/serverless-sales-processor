-- Revenue and quantity per product per month, across every completed upload.
-- Duplicate uploads and failed files never reach this table, so nothing is
-- double counted. Product names are compared case-insensitively and shown
-- under their alphabetically first spelling, the same rule the reports use.
SELECT
  month,
  min(product) AS product,
  SUM(quantity) AS quantity,
  SUM(revenue) AS revenue
FROM sales
GROUP BY month, lower(product)
ORDER BY month, revenue DESC;

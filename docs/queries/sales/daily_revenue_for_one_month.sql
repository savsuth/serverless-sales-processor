-- Daily revenue for one month. Change the month to explore another.
SELECT
  "date",
  SUM(revenue) AS revenue,
  COUNT(*) AS sale_rows
FROM sales
WHERE month = '2024-01'
GROUP BY "date"
ORDER BY "date";

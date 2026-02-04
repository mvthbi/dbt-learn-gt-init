SELECT 
    order_id,
    SUM(amount) AS total_amount
FROM analytics.dbt_mvthbi.stg_stripe__payments
GROUP BY 1
HAVING SUM(amount) < 10
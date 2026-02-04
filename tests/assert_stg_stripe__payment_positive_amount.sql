SELECT 
    order_id,
    SUM(amount) AS total_amount
FROM {{ref('stg_stripe__payments')}}
GROUP BY 1
HAVING SUM(amount) < 0
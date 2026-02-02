SELECT 
    id AS payment_id,
    orderid AS order_id,
    paymentmethod AS payment_method,
    status,
    amount,
    created AS created_date
FROM raw.stripe.payment
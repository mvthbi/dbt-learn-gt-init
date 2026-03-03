-- import
with customers as (
    select * from {{ref('stg_jaffle_shop__customers')}}
),

orders as (
    select * from {{ref('stg_jaffle_shop__orders')}}
),

payments as (
    select * from {{ref('stg_stripe__payments')}}
),

-- logic
paid_payments as (
    select 
        order_id, 
        max(created) as payment_finalized_date, 
        sum(amount) as total_amount_paid
    from payments
    where status <> 'fail'
    group by 1
),

paid_orders as (
    select 
        orders.order_id,
        orders.customer_id,
        orders.order_Date as order_placed_at,
        orders.status as order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        c.first_name as customer_first_name,
        c.last_name as customer_last_name
    from orders
    left join paid_payments p on orders.order_id = p.order_id
    left join customers as c on orders.customer_id = c.customer_id 
),

customer_orders as (
    select 
        customers.customer_id
        , min(order_date) as first_order_date
        , max(order_date) as most_recent_order_date
        , count(order_id) as number_of_orders
    from customers
    left join orders on orders.customer_id = customers.customer_id 
    group by 1
),

paid_order_and_payment as (
    select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
    from paid_orders p
    left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
    group by 1
),

-- final
final as (
    select
        paid_orders.*,
        row_number() over (order by paid_orders.order_id) as transaction_seq,
        row_number() over (partition by paid_orders.customer_id order by paid_orders.order_id) as customer_sales_seq,
        case 
            when customer_orders.first_order_date = paid_orders.order_placed_at
            then 'new'
            else 'return' end as nvsr,
        paid_order_and_payment.clv_bad as customer_lifetime_value,
        customer_orders.first_order_date as fdos
    from paid_orders
    left join customer_orders using (customer_id)
    left outer join paid_order_and_payment on paid_order_and_payment.order_id = paid_orders.order_id
    order by paid_orders.order_id
)

select * from final
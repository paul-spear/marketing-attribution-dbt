select 
    customer_id,
    email,
    first_order_date::timestamp as converted_at,
    first_order_value as revenue
from {{ source('raw_data', 'customers') }}
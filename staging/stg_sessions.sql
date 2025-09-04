select 
    session_id,
    customer_id,
    started_at::timestamp as started_at,
    utm_source,
    utm_medium,
    utm_campaign,
    session_key
from {{ source('raw_data', 'sessions') }}
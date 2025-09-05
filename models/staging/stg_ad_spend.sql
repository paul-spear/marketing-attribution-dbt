select 
    date_day::date as date_day,
    utm_source,
    utm_medium,
    utm_campaign,
    spend
from {{ source('raw_data', 'ad_spend') }}
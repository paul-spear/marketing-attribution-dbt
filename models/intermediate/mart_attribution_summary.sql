{{ config(materialized='table') }}

with attribution_touches as (
    select * from {{ ref('int_attribution_touches') }}
),

ad_spend as (
    select * from {{ ref('stg_ad_spend') }}
),

-- Aggregate attribution data by month and channel
attribution_aggregated as (
    select
        date_trunc('month', converted_at) as date_month,
        utm_source,
        utm_medium,
        utm_campaign,
        count(distinct customer_id) as customers,
        count(*) as total_touches,
        sum(first_touch_points) as first_touch_conversions,
        sum(last_touch_points) as last_touch_conversions,
        sum(linear_points) as linear_conversions,
        sum(forty_twenty_forty_points) as forty_twenty_forty_conversions,
        sum(first_touch_revenue) as first_touch_revenue,
        sum(last_touch_revenue) as last_touch_revenue,
        sum(linear_revenue) as linear_revenue,
        sum(forty_twenty_forty_revenue) as forty_twenty_forty_revenue
    from attribution_touches
    group by date_month,
             utm_source,
             utm_medium,
             utm_campaign

-- Aggregate ad spend by month and channel
ad_spend_aggregated as (
    select
        date_trunc('month', date_day) as date_month,
        utm_source,
        utm_medium,
        utm_campaign,
        sum(spend) as total_spend
    from ad_spend
    group by date_month,
             utm_source,
             utm_medium,
             utm_campaign
),

-- Join attribution and spend data
final as (
    select
        coalesce(a.date_month, s.date_month) as date_month,
        coalesce(a.utm_source, s.utm_source) as utm_source,
        coalesce(a.utm_medium, s.utm_medium) as utm_medium,
        coalesce(a.utm_campaign, s.utm_campaign) as utm_campaign,
        
        -- Attribution metrics
        coalesce(a.customers, 0) as customers,
        coalesce(a.total_touches, 0) as total_touches,
        coalesce(a.linear_conversions, 0) as linear_conversions,
        coalesce(a.linear_revenue, 0) as linear_revenue,
        coalesce(a.forty_twenty_forty_conversions, 0) as forty_twenty_forty_conversions,
        coalesce(a.forty_twenty_forty_revenue, 0) as forty_twenty_forty_revenue,
        
        -- Spend metrics
        coalesce(s.total_spend, 0) as total_spend,
        
        -- Performance metrics
        case 
            when coalesce(a.linear_conversions, 0) > 0 
            then coalesce(s.total_spend, 0) / a.linear_conversions 
            else null 
        end as cost_per_acquisition_linear,
        
        case 
            when coalesce(s.total_spend, 0) > 0 
            then coalesce(a.linear_revenue, 0) / s.total_spend 
            else null 
        end as return_on_ad_spend_linear
            
    from attribution_aggregated a
    full outer join ad_spend_aggregated s
        on a.date_month = s.date_month
        and a.utm_source = s.utm_source
        and a.utm_medium = s.utm_medium
        and a.utm_campaign = s.utm_campaign
)

select * from final
order by date_month, utm_source, utm_campaign
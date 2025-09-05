{{ config(materialized='table') }}

with sessions as (
    select * from {{ ref('stg_sessions') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

-- Join sessions with conversions and filter to pre-conversion sessions
sessions_before_conversion as (
    select
        s.*,
        c.converted_at,
        c.revenue
    from sessions s
    inner join customers c using (customer_id)
    where s.started_at <= c.converted_at
    and s.started_at >= dateadd(days, -30, c.converted_at)  -- 30-day attribution window
),

-- Calculate session metrics for attribution
sessions_with_position as (
    select
        *,
        count(*) over (partition by customer_id) as total_sessions,
        row_number() over (partition by customer_id order by started_at) as session_index
    from sessions_before_conversion
)

select * from sessions_with_position
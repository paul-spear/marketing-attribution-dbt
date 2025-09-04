{{ config(materialized='table') }}

with sessions_before_conversion as (
    select * from {{ ref('int_sessions_before_conversion') }}
),

-- Calculate attribution points using different methods
attribution_points as (
    select
        *,
        -- First touch: 100% to first session
        case 
            when session_index = 1 then 1.0 
            else 0.0 
        end as first_touch_points,
        
        -- Last touch: 100% to last session  
        case 
            when session_index = total_sessions then 1.0 
            else 0.0 
        end as last_touch_points,
        
        -- Linear: Equal distribution across all sessions
        1.0 / total_sessions as linear_points,
        
        -- Forty-twenty-forty: 40% first, 40% last, 20% distributed among middle
        case
            when total_sessions = 1 then 1.0
            when total_sessions = 2 then 0.5
            when session_index = 1 then 0.4
            when session_index = total_sessions then 0.4
            else 0.2 / (total_sessions - 2)
        end as forty_twenty_forty_points
        
    from sessions_before_conversion
),

-- Calculate attributed revenue
final as (
    select
        *,
        revenue * first_touch_points as first_touch_revenue,
        revenue * last_touch_points as last_touch_revenue,
        revenue * linear_points as linear_revenue,
        revenue * forty_twenty_forty_points as forty_twenty_forty_revenue
    from attribution_points
)

select * from final
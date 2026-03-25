{{ config(materialized='incremental', unique_key='event_id') }}

select
    event_id,
    user_id,
    course_id,
    event_type,
    event_ts::timestamp as event_ts,
    time_spent_sec,
    rating,
    device_type,
    app_version,
    user_agent,
    ip_address,
    session_id,
    {{ dbt_utils.generate_surrogate_key(['event_id']) }} as event_sk
from {{ source('raw', 'events') }}

{% if is_incremental() %}
  where event_ts > (select max(event_ts) from {{ this }})
{% endif %}
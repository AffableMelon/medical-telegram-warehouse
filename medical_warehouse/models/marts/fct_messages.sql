with stg_messages as (
    select * from {{ ref('stg_telegram_messages') }}
),

dim_channels as (
    select * from {{ ref('dim_channels') }}
),

dim_dates as (
    select * from {{ ref('dim_dates') }}
)

select
    m.message_id,
    c.channel_key,
    d.date_key,
    m.message_text,
    m.message_length,
    m.view_count,
    m.forward_count,
    m.has_image
from stg_messages m
left join dim_channels c on m.channel_name = c.channel_name
left join dim_dates d on date(m.message_date) = d.full_date

with stg_messages as (
    select * from {{ ref('stg_telegram_messages') }}
),

channel_stats as (
    select
        channel_name,
        min(message_date) as first_post_date,
        max(message_date) as last_post_date,
        count(*) as total_posts,
        avg(view_count) as avg_views
    from stg_messages
    group by 1
)

select
    row_number() over (order by channel_name) as channel_key,
    channel_name,
    'Unknown' as channel_type, -- Placeholder as not in raw data
    first_post_date,
    last_post_date,
    total_posts,
    avg_views
from channel_stats

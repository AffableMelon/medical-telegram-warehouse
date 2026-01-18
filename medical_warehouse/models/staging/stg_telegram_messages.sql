with source as (
    select * from {{ source('raw', 'telegram_messages') }}
),

renamed as (
    select
        id as raw_id,
        channel_name,
        message_id,
        cast(date as timestamp) as message_date,
        message_text,
        coalesce(views, 0) as view_count,
        coalesce(forwards, 0) as forward_count,
        media_path
    from source
    where message_id is not null
),

final as (
    select
        *,
        length(message_text) as message_length,
        case when media_path is not null then true else false end as has_image
    from renamed
)

select * from final

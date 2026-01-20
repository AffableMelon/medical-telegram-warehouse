with detections as (
    select * from {{ source('raw', 'yolo_detections') }}
),

categories as (
    select * from {{ source('raw', 'image_categories') }}
),

messages as (
    select * from {{ ref('fct_messages') }}
)

select
    m.message_id,
    m.channel_key,
    m.date_key,
    d.detected_class,
    d.confidence,
    c.category as image_category
from messages m
inner join detections d on m.message_id = d.message_id
left join categories c on m.message_id = c.message_id


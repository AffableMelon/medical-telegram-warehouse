with detections as (
    select * from {{ source('raw', 'yolo_detections') }}
),

messages as (
    select * from {{ ref('fct_messages') }}
),

path_parsing as (
    select
        id as detection_id,
        image_path,
        detected_class,
        confidence,
        -- Extract message_id from path: .../channel/123.jpg -> 123
        -- Reverse the string, take first part (filename), reverse back, then take first part before dot.
        -- OR more simply: use regexp_replace to extract the number before .jpg
        substring(image_path from '([0-9]+)\.jpg')::bigint as message_id_extracted
    from detections
),

aggregated_objects as (
    select
        image_path,
        bool_or(detected_class = 'person') as has_person,
        bool_or(detected_class = 'bottle') as has_bottle,
        max(case when detected_class is null then 1 else 0 end) as is_no_detection
    from path_parsing
    group by 1
),

classified as (
    select
        image_path,
        case 
            when has_person and has_bottle then 'promotional'
            when has_bottle and not has_person then 'product_display'
            when has_person and not has_bottle then 'lifestyle'
            else 'other'
        end as image_category
    from aggregated_objects
)

select
    m.message_id,
    m.channel_key,
    m.date_key,
    p.detected_class,
    p.confidence,
    c.image_category
from path_parsing p
left join messages m on p.message_id_extracted = m.message_id
left join classified c on p.image_path = c.image_path
where m.message_id is not null

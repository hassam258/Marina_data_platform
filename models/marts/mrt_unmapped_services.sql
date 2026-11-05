{{
    config(
        materialized='table',
        tags=['marts', 'governance']
    )
}}

/*
    Ranked vocabulary of service strings with no seed mapping. Grain: one row per
    raw_service_type.

    This is the feedback loop that keeps the seed alive. The flag on the staging model
    tells you THAT something is unmapped; this tells you WHAT, ranked by volume, so the
    monthly maintenance job is "read the top ten and add six rows" rather than "go spelunk
    a failures table".
*/

select
    raw_service_type,
    count(*)                                        as job_count,
    count(distinct vendor_name)                     as vendor_count,
    sum(hours_spent)                                as total_hours,
    min(created_at_utc)                             as first_seen_at_utc,
    max(created_at_utc)                             as last_seen_at_utc
from {{ ref('stg_vendor_work_orders') }}
where is_unmapped_service
  and raw_service_type is not null
group by raw_service_type
order by job_count desc

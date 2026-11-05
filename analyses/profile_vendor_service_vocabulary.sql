-- Run this against RAW before populating the seed. The seed is built from observed
-- vocabulary, not from imagination.
select
    lower(trim(raw_service_type))   as raw_key,
    count(*)                        as job_count,
    count(distinct vendor_name)     as vendor_count,
    min(created_at)                 as first_seen,
    max(created_at)                 as last_seen
from {{ source('marina_raw', 'raw_vendor_work_orders') }}
group by 1
order by job_count desc

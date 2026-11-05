-- Reconciliation between landing and staging. Staging keeps the latest version per key, so
-- its row count must equal the DISTINCT KEY count in landing. A mismatch means the dedupe
-- window dropped something or the watermark skipped a batch.
{{ config(severity='error', tags=['reconciliation']) }}

with landing_keys as (
    select count(distinct work_order_id) as n from {{ ref('raw_vendor_work_orders') }}
),
staging_keys as (
    select count(*) as n from {{ ref('stg_vendor_work_orders') }}
)
select l.n as landing_distinct_keys, s.n as staging_rows
from landing_keys l cross join staging_keys s
where l.n <> s.n

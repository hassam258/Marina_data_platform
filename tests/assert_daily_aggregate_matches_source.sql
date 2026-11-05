-- Reconciliation: the aggregate tier must agree with the fact table it summarizes, on the
-- window both cover. Aggregates drifting from their source is the classic way a mart
-- becomes confidently wrong.
{{ config(severity='error', tags=['reconciliation']) }}

with fact as (
    select job_date, count(*) as jobs
    from {{ ref('stg_vendor_work_orders') }}
    where job_date >= cast({{ dbt.dateadd('day', -1 * var('aggregate_lookback_days'), dbt.current_timestamp()) }} as date)
    group by job_date
),
agg as (
    select job_date, sum(jobs_logged) as jobs
    from {{ ref('int_vendor_jobs_daily') }}
    where job_date >= cast({{ dbt.dateadd('day', -1 * var('aggregate_lookback_days'), dbt.current_timestamp()) }} as date)
    group by job_date
)
select
    coalesce(f.job_date, a.job_date) as job_date,
    f.jobs as fact_jobs,
    a.jobs as aggregate_jobs
from fact f
full outer join agg a on f.job_date = a.job_date
where coalesce(f.jobs, -1) <> coalesce(a.jobs, -1)

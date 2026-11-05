{{
    config(
        materialized='incremental',
        unique_key=['job_date', 'vendor_name', 'service_category'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        partition_by=daily_partition('job_date'),
        cluster_by=['vendor_name'],
        tags=['intermediate', 'vendors', 'aggregate']
    )
}}

/*
    Daily vendor aggregate. Grain: job_date x vendor_name x service_category.

    THE POINT OF THIS MODEL: the mart never touches the billion-row fact table. Roughly
    500 vendors x 5 categories x 365 days is under a million rows a year, so every
    executive query runs against something small regardless of how many billions sit
    underneath.

    ONLY ADDITIVE MEASURES ARE STORED. sum(hours), count(jobs), sum(turnaround),
    count(turnaround) - never avg(). An average cannot be re-aggregated across days; a sum
    and a count can. Storing pre-computed averages here would make every rollup silently
    wrong (the average of daily averages is not the period average).

    Percentiles are deliberately absent for the same reason: they are not additive.
    Computing p90 at scale needs the raw table on a bounded window, or a mergeable sketch
    (t-digest / approx_percentile state), which is the next step if the business asks.
*/

with work_orders as (

    select * from {{ ref('stg_vendor_work_orders') }}
    {% if is_incremental() %}
    where job_date >= cast({{ dbt.dateadd('day', -1 * var('aggregate_lookback_days'), dbt.current_timestamp()) }} as date)
    {% endif %}

)

select
    job_date,
    coalesce(vendor_name, 'Unknown Vendor')                               as vendor_name,
    service_category,

    count(*)                                                              as jobs_logged,
    count(case when is_completed then 1 end)                              as jobs_completed,
    count(case when is_unmapped_service then 1 end)                       as jobs_unmapped_service,

    sum(case when is_completed and is_billable then hours_spent end)      as billable_hours,
    sum(hours_spent)                                                      as total_hours,

    sum(turnaround_hours)                                                 as turnaround_hours_sum,
    count(turnaround_hours)                                               as turnaround_hours_count,
    min(turnaround_hours)                                                 as turnaround_hours_min,
    max(turnaround_hours)                                                 as turnaround_hours_max,

    {{ dbt.current_timestamp() }}                                         as dbt_updated_at
from work_orders
group by job_date, coalesce(vendor_name, 'Unknown Vendor'), service_category

{{
    config(
        materialized='incremental',
        unique_key='kpi_key',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        partition_by=daily_partition('metric_date'),
        cluster_by=['metric_domain'],
        tags=['marts', 'executive']
    )
}}

/*
    Executive KPI mart. Grain: metric_date x metric_domain x metric_grain x grain_value
                              x metric_name.

    TWO CHANGES FROM A NAIVE MART, both about scale:

    1. IT KEEPS HISTORY. v1 stamped current_date and rebuilt everything, so you got today's
       MRR and no trend - and the first question any executive asks is "is it going up".
       This appends daily and merges only the recent window, so five years of history costs
       nothing to retain and nothing to recompute.

    2. IT READS AGGREGATES, NOT FACTS. Vendor metrics come from int_vendor_jobs_daily and
       revenue from int_subscription_state. Neither the billion-row work order table nor
       the billion-row event log is scanned here.

    Long/tidy shape: the KPI families sit at different grains (company revenue vs per
    vendor operations), so a wide table would NULL-pad and break the primary key. Adding a
    metric is a UNION ALL, not a schema migration.

    Revenue rows are stamped with the CURRENT date only - int_subscription_state is a
    point-in-time snapshot, so back-dating it would be a lie. Historical MRR needs a daily
    snapshot of subscription state, which is the next model to add if the business wants
    MRR trend rather than MRR today.
*/

with vendor_daily as (

    select * from {{ ref('int_vendor_jobs_daily') }}
    {% if is_incremental() %}
    where job_date >= cast({{ dbt.dateadd('day', -1 * var('aggregate_lookback_days'), dbt.current_timestamp()) }} as date)
    {% endif %}

),

-- Period rollups answer the brief's questions directly: "total billable hours over
-- total logged jobs" and "average turnaround" are single figures, not daily series.
-- Read without the incremental date filter: the aggregate tier is small by design, so
-- scanning all of it is cheap, and a period figure computed from a 7-day window would
-- be wrong.
vendor_period as (

    select
        vendor_name,
        sum(jobs_logged)            as jobs_logged,
        sum(jobs_completed)         as jobs_completed,
        sum(billable_hours)         as billable_hours,
        sum(turnaround_hours_sum)   as turnaround_hours_sum,
        sum(turnaround_hours_count) as turnaround_hours_count
    from {{ ref('int_vendor_jobs_daily') }}
    group by vendor_name

),

company_period as (

    select
        sum(jobs_logged)            as jobs_logged,
        sum(jobs_completed)         as jobs_completed,
        sum(billable_hours)         as billable_hours,
        sum(turnaround_hours_sum)   as turnaround_hours_sum,
        sum(turnaround_hours_count) as turnaround_hours_count,
        count(distinct vendor_name) as active_vendors
    from {{ ref('int_vendor_jobs_daily') }}

),

subscriptions as (

    select
        s.sub_id,
        s.is_active_event,
        s.normalized_mrr_amount,
        x.owner_id
    from {{ ref('int_subscription_state') }} s
    left join {{ ref('int_owner_identity_xref') }} x
        on x.identity_source = 'stripe' and x.source_record_id = s.sub_id

),

revenue as (

    select
        cast({{ dbt.current_timestamp() }} as date)                                  as metric_date,
        coalesce(sum(case when is_active_event then normalized_mrr_amount end), 0)   as total_mrr,
        count(distinct case when is_active_event then sub_id end)                    as active_subscriptions,
        count(distinct case when is_active_event then owner_id end)                  as paying_owners,
        count(distinct case when not is_active_event then sub_id end)                as churned_subscriptions
    from subscriptions

),

vendor_by_day as (

    select
        job_date,
        vendor_name,
        sum(jobs_logged)             as jobs_logged,
        sum(jobs_completed)          as jobs_completed,
        sum(billable_hours)          as billable_hours,
        sum(turnaround_hours_sum)    as turnaround_hours_sum,
        sum(turnaround_hours_count)  as turnaround_hours_count
    from vendor_daily
    group by job_date, vendor_name

),

company_by_day as (

    select
        job_date,
        sum(jobs_logged)             as jobs_logged,
        sum(jobs_completed)          as jobs_completed,
        sum(billable_hours)          as billable_hours,
        sum(turnaround_hours_sum)    as turnaround_hours_sum,
        sum(turnaround_hours_count)  as turnaround_hours_count,
        count(distinct vendor_name)  as active_vendors
    from vendor_daily
    group by job_date

),

metrics_long as (

    -- ---------- Revenue (company grain, current snapshot) ----------
    select metric_date, 'Revenue' as metric_domain, 'company' as metric_grain,
           'All Marinas' as grain_value, 'monthly_recurring_revenue' as metric_name,
           cast(total_mrr as decimal(18, 2)) as metric_value, 'USD' as metric_unit
    from revenue

    union all
    select metric_date, 'Revenue', 'company', 'All Marinas', 'annual_recurring_revenue',
           cast(total_mrr * 12 as decimal(18, 2)), 'USD'
    from revenue

    union all
    select metric_date, 'Revenue', 'company', 'All Marinas', 'active_subscriptions',
           cast(active_subscriptions as decimal(18, 2)), 'count'
    from revenue

    union all
    select metric_date, 'Revenue', 'company', 'All Marinas', 'paying_owners',
           cast(paying_owners as decimal(18, 2)), 'count'
    from revenue

    union all
    select metric_date, 'Revenue', 'company', 'All Marinas', 'average_revenue_per_account',
           cast({{ dbt_utils.safe_divide('total_mrr', 'nullif(active_subscriptions, 0)') }} as decimal(18, 2)), 'USD'
    from revenue

    -- ---------- Vendor operations (company grain, per day) ----------
    union all
    select job_date, 'Vendor Operations', 'company', 'All Vendors', 'total_billable_hours',
           cast(billable_hours as decimal(18, 2)), 'hours'
    from company_by_day

    union all
    select job_date, 'Vendor Operations', 'company', 'All Vendors', 'total_completed_jobs',
           cast(jobs_completed as decimal(18, 2)), 'count'
    from company_by_day

    union all
    select job_date, 'Vendor Operations', 'company', 'All Vendors', 'hours_per_logged_job',
           cast({{ dbt_utils.safe_divide('billable_hours', 'nullif(jobs_logged, 0)') }} as decimal(18, 2)), 'hours'
    from company_by_day

    union all
    select job_date, 'Vendor Operations', 'company', 'All Vendors', 'active_vendors',
           cast(active_vendors as decimal(18, 2)), 'count'
    from company_by_day

    union all
    select job_date, 'Vendor Response Time', 'company', 'All Vendors', 'avg_turnaround_hours',
           cast({{ dbt_utils.safe_divide('turnaround_hours_sum', 'nullif(turnaround_hours_count, 0)') }} as decimal(18, 2)), 'hours'
    from company_by_day

    -- ---------- Vendor utilization, PERIOD total (the brief's requirement 2) ----------
    union all
    select cast({{ dbt.current_timestamp() }} as date), 'Vendor Operations', 'period', 'All Vendors',
           'total_billable_hours', cast(billable_hours as decimal(18, 2)), 'hours'
    from company_period

    union all
    select cast({{ dbt.current_timestamp() }} as date), 'Vendor Operations', 'period', 'All Vendors',
           'total_logged_jobs', cast(jobs_logged as decimal(18, 2)), 'count'
    from company_period

    union all
    select cast({{ dbt.current_timestamp() }} as date), 'Vendor Operations', 'period', 'All Vendors',
           'total_completed_jobs', cast(jobs_completed as decimal(18, 2)), 'count'
    from company_period

    union all
    select cast({{ dbt.current_timestamp() }} as date), 'Vendor Operations', 'period', 'All Vendors',
           'hours_per_logged_job',
           cast({{ dbt_utils.safe_divide('billable_hours', 'nullif(jobs_logged, 0)') }} as decimal(18, 2)), 'hours'
    from company_period

    union all
    select cast({{ dbt.current_timestamp() }} as date), 'Vendor Operations', 'period', vendor_name,
           'billable_hours', cast(billable_hours as decimal(18, 2)), 'hours'
    from vendor_period

    union all
    select cast({{ dbt.current_timestamp() }} as date), 'Vendor Operations', 'period', vendor_name,
           'total_logged_jobs', cast(jobs_logged as decimal(18, 2)), 'count'
    from vendor_period

    union all
    select cast({{ dbt.current_timestamp() }} as date), 'Vendor Operations', 'period', vendor_name,
           'hours_per_logged_job',
           cast({{ dbt_utils.safe_divide('billable_hours', 'nullif(jobs_logged, 0)') }} as decimal(18, 2)), 'hours'
    from vendor_period

    -- ---------- Vendor response time, PERIOD average (the brief's requirement 3) ----------
    -- Rebuilt from additive components: sum(turnaround) / count(turnaround). Averaging
    -- daily averages would be wrong the moment two days have different job counts.
    union all
    select cast({{ dbt.current_timestamp() }} as date), 'Vendor Response Time', 'period', 'All Vendors',
           'avg_turnaround_hours',
           cast({{ dbt_utils.safe_divide('turnaround_hours_sum', 'nullif(turnaround_hours_count, 0)') }} as decimal(18, 2)), 'hours'
    from company_period

    union all
    select cast({{ dbt.current_timestamp() }} as date), 'Vendor Response Time', 'period', vendor_name,
           'avg_turnaround_hours',
           cast({{ dbt_utils.safe_divide('turnaround_hours_sum', 'nullif(turnaround_hours_count, 0)') }} as decimal(18, 2)), 'hours'
    from vendor_period

    -- ---------- Vendor operations (vendor grain, per day) ----------
    union all
    select job_date, 'Vendor Operations', 'vendor', vendor_name, 'billable_hours',
           cast(billable_hours as decimal(18, 2)), 'hours'
    from vendor_by_day

    union all
    select job_date, 'Vendor Operations', 'vendor', vendor_name, 'completed_jobs',
           cast(jobs_completed as decimal(18, 2)), 'count'
    from vendor_by_day

    union all
    select job_date, 'Vendor Operations', 'vendor', vendor_name, 'hours_per_completed_job',
           cast({{ dbt_utils.safe_divide('billable_hours', 'nullif(jobs_completed, 0)') }} as decimal(18, 2)), 'hours'
    from vendor_by_day

    union all
    select job_date, 'Vendor Operations', 'vendor', vendor_name, 'hours_per_logged_job',
           cast({{ dbt_utils.safe_divide('billable_hours', 'nullif(jobs_logged, 0)') }} as decimal(18, 2)), 'hours'
    from vendor_by_day

    union all
    select job_date, 'Vendor Operations', 'vendor', vendor_name, 'completion_rate',
           cast({{ dbt_utils.safe_divide('jobs_completed', 'nullif(jobs_logged, 0)') }} as decimal(18, 4)), 'ratio'
    from vendor_by_day

    union all
    select job_date, 'Vendor Response Time', 'vendor', vendor_name, 'avg_turnaround_hours',
           cast({{ dbt_utils.safe_divide('turnaround_hours_sum', 'nullif(turnaround_hours_count, 0)') }} as decimal(18, 2)), 'hours'
    from vendor_by_day

)

select
    {{ dbt_utils.generate_surrogate_key(['metric_date', 'metric_domain', 'metric_grain', 'grain_value', 'metric_name']) }} as kpi_key,
    metric_date,
    metric_domain,
    metric_grain,
    grain_value,
    metric_name,
    metric_value,
    metric_unit,
    {{ dbt.current_timestamp() }} as dbt_updated_at
from metrics_long

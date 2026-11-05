{{
    config(
        materialized='incremental',
        unique_key='work_order_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        partition_by=daily_partition('created_at_utc'),
        cluster_by=['vendor_name', 'status'],
        tags=['staging', 'vendors', 'high_volume']
    )
}}

/*
    Staging: vendor work orders. Grain: one row per work_order_id.
    THE billion-row table. Every scaling decision in this project is aimed here.

      partition_by            daily on created_at_utc (BigQuery)
      cluster_by              (vendor_name, status) - what the scorecard filters on
      No incremental_predicates, on purpose. That setting tells MERGE to only look at
                                recent rows in the target table, which is faster. But if a row it needs to update
                                is older than that window, MERGE does not find it and inserts a second copy
                                instead. Duplicates, no error. Not worth the speed.
      rolling window          reprocesses late vendor paperwork instead of dropping it

    NOTE: this table joins to nothing. Source B carries no hull_id and no customer
    reference, so work orders cannot be attributed to an owner or a boat. That is a
    source-system gap, not a modelling choice - see README > Known data gaps.
*/

with landed as (

    select * from {{ ref('raw_vendor_work_orders') }}
    where work_order_id is not null
    {% if is_incremental() %}
      and {{ landing_watermark_filter('_ingested_at') }}
    {% endif %}

),

source as (

    -- A job that moves from In Progress to Completed lands twice. Keep the latest version.
    select * from landed
    qualify row_number() over (
        partition by work_order_id
        order by _ingested_at desc, _row_hash desc
    ) = 1

),

service_map as (

    select
        lower(trim(raw_service_type)) as raw_service_type_key,
        service_category,
        is_billable
    from {{ ref('seed_vendor_service_map') }}

),

cleaned as (

    select
        trim(cast(work_order_id as {{ dbt.type_string() }}))                 as work_order_id,
        {{ clean_name('vendor_name') }}                                      as vendor_name,
        nullif(trim(cast(raw_service_type as {{ dbt.type_string() }})), '')  as raw_service_type,
        lower(trim(cast(raw_service_type as {{ dbt.type_string() }})))       as raw_service_type_key,
        initcap(trim(cast(status as {{ dbt.type_string() }})))               as status,
        cast(nullif(regexp_replace(cast(hours_spent as {{ dbt.type_string() }}), '[^0-9.]', ''), '')
             as decimal(18, 2))                                              as hours_spent,
        {{ to_utc_timestamp('created_at') }}                                 as created_at_utc,
        {{ to_utc_timestamp('completed_at') }}                               as completed_at_utc,
        _row_hash,
        _ingested_at,
        _batch_id
    from source

),

final as (

    select
        c.work_order_id,
        c.vendor_name,
        c.raw_service_type,
        coalesce(m.service_category, 'Other')                                as service_category,
        coalesce(m.is_billable, true)                                        as is_billable,
        (m.service_category is null)                                         as is_unmapped_service,
        c.status,
        c.hours_spent,
        c.created_at_utc,
        c.completed_at_utc,
        cast(c.created_at_utc as date)                                       as job_date,
        case
            when c.status = 'Completed' and c.completed_at_utc is not null
                then {{ hours_between('c.created_at_utc', 'c.completed_at_utc') }}
        end                                                                  as turnaround_hours,
        (c.status = 'Completed')                                             as is_completed,
        c._row_hash,
        c._ingested_at,
        c._batch_id
    from cleaned c
    left join service_map m on c.raw_service_type_key = m.raw_service_type_key

)

-- No business-date filter here: the landing watermark already bounds this run to newly
-- landed rows, whatever business date they carry. That is what makes a work order filed
-- three weeks late still get processed.
select * from final

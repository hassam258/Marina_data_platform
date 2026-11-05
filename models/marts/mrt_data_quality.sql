{{
    config(
        materialized='incremental',
        unique_key='check_key',
        incremental_strategy='merge',
        partition_by=daily_partition('check_date'),
        tags=['marts', 'governance']
    )
}}

/*
    Data quality scoreboard. Grain: check_date x check_name.

    At billions of rows you cannot run `unique` on every key every day, so quality
    management shifts from assertion to OBSERVATION: track rates over time and alert on
    drift. This table is what makes that possible - it is the thing a human actually looks
    at, rather than a failures table nobody opens.
*/

with checks as (

    select 'unparseable_contact' as check_name, 'stg_marina_pms' as model_name,
           count(case when contact_type = 'unparseable' then 1 end) as failing_records,
           count(*) as total_records
    from {{ ref('stg_marina_pms') }}

    union all
    select 'unmapped_service_type', 'stg_vendor_work_orders',
           count(case when is_unmapped_service then 1 end), count(*)
    from {{ ref('stg_vendor_work_orders') }}

    union all
    select 'poisoned_identity_key', 'int_identity_keys',
           count(case when is_poisoned_key then 1 end), count(*)
    from {{ ref('int_identity_keys') }}

    union all
    select 'unresolved_identity', 'int_owner_identity_xref',
           count(case when resolution_method = 'unresolved' then 1 end), count(*)
    from {{ ref('int_owner_identity_xref') }}

    union all
    select 'orphan_boat', 'int_boats',
           count(case when is_orphan_boat then 1 end), count(*)
    from {{ ref('int_boats') }}

    union all
    select 'implausible_hull_id', 'int_boats',
           count(case when not has_plausible_hull_id then 1 end), count(*)
    from {{ ref('int_boats') }}

    union all
    select 'boat_with_multiple_owners', 'int_boats',
           count(case when has_ownership_history then 1 end), count(*)
    from {{ ref('int_boats') }}

)

select
    {{ dbt_utils.generate_surrogate_key(["cast(" ~ dbt.current_timestamp() ~ " as date)", 'check_name']) }} as check_key,
    cast({{ dbt.current_timestamp() }} as date)                          as check_date,
    check_name,
    model_name,
    failing_records,
    total_records,
    cast({{ dbt_utils.safe_divide('failing_records', 'nullif(total_records, 0)') }} as decimal(18, 6)) as failing_rate
from checks

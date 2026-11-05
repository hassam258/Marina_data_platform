{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='append_new_columns',
        partition_by=daily_partition('_ingested_at'),
        cluster_by=['vendor_name'],
        tags=['raw', 'landing', 'high_volume']
    )
}}

/*
    Landing: vendor work orders. Append-only, immutable, untyped.

    THE billion-row table. In production this model is an external table over the loader's
    Iceberg/Delta output rather than a dbt-managed copy - dbt should not be the thing
    physically moving billions of rows. The columns and the contract are identical either
    way, which is what makes that swap a config change.

    An in-progress job that later completes lands twice (status and completed_at both
    change, so the hash changes). Staging keeps the latest version per work_order_id.
*/

with raw_payload as (

    select
        work_order_id,
        vendor_name,
        raw_service_type,
        hours_spent,
        status,
        created_at,
        completed_at,
        {{ landing_metadata([
            'work_order_id', 'vendor_name', 'raw_service_type',
            'hours_spent', 'status', 'created_at', 'completed_at'
        ]) }}
    from {{ source('marina_raw', 'raw_vendor_work_orders') }}

)

select * from raw_payload
where 1 = 1
{{ landing_dedupe_filter() }}

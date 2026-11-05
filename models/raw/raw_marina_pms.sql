{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='append_new_columns',
        partition_by=daily_partition('_ingested_at'),
        tags=['raw', 'landing', 'pii']
    )
}}

/*
    Landing: marina PMS. Append-only, immutable, untyped.

    Every column is passed through EXACTLY as received - no casting, no trimming, no
    renaming. If slip_fee arrives as '$1,200.00' it is stored as '$1,200.00'. Cleaning it
    here would destroy the audit trail and make it impossible to prove what the source
    actually sent.

    Bookings are mutable in the PMS, so a changed booking lands as a NEW ROW with a new
    _row_hash. This table is therefore the full change history of every booking, and
    stg_marina_pms selects the latest version per booking_id.
*/

with raw_payload as (

    select
        booking_id,
        customer_full_name,
        raw_contact,
        boat_name,
        hull_id,
        slip_fee,
        marina_code,
        {{ landing_metadata([
            'booking_id', 'customer_full_name', 'raw_contact',
            'boat_name', 'hull_id', 'slip_fee', 'marina_code'
        ]) }}
    from {{ source('marina_raw', 'raw_marina_pms') }}

)

select * from raw_payload
where 1 = 1
{{ landing_dedupe_filter() }}

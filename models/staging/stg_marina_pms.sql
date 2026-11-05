{{
    config(
        materialized='incremental',
        unique_key='booking_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        cluster_by=['marina_code'],
        tags=['staging', 'pms', 'pii'],
        post_hook=[
            "{{ apply_masking_policy('marina_name_mask', ['customer_full_name']) }}",
            "{{ apply_masking_policy('marina_email_mask', ['customer_email']) }}",
            "{{ apply_masking_policy('marina_phone_mask', ['customer_phone']) }}",
        ]
    )
}}

/*
    Staging: marina PMS bookings. Grain: one row per booking_id.

    `raw_contact` is one unlabelled column holding EITHER an email OR a phone. Rather
    than classifying then extracting, both extractors run on every row - each returns
    NULL on a pattern miss, so the column self-classifies:

        '+1 (555) 019-2834' -> phone only (no email pattern)
        'jdoe_99@gmail.com' -> email only (longest digit run is '99', below the floor)

    Incremental because bookings are updated in place (status, dates, fees) and a full
    rebuild of a billion-row table is not a thing you do daily.
*/

with landed as (

    select * from {{ ref('raw_marina_pms') }}
    where booking_id is not null
    {% if is_incremental() %}
      and {{ landing_watermark_filter('_ingested_at') }}
    {% endif %}

),

source as (

    -- latest version per key; _row_hash breaks ties deterministically when two versions
    -- share an ingestion timestamp.
    select * from landed
    qualify row_number() over (
        partition by booking_id
        order by _ingested_at desc, _row_hash desc
    ) = 1

),

cleaned as (

    select
        trim(cast(booking_id as {{ dbt.type_string() }}))              as booking_id,
        upper(trim(cast(marina_code as {{ dbt.type_string() }})))      as marina_code,

        {{ clean_name('customer_full_name') }}                         as customer_full_name,
        {{ normalize_email('raw_contact') }}                           as customer_email,
        {{ normalize_phone('raw_contact') }}                           as customer_phone,
        raw_contact                                                    as raw_contact,

        {{ clean_name('boat_name') }}                                  as boat_name,
        {{ clean_hull_id('hull_id') }}                                 as hull_id,
        hull_id                                                        as hull_id_raw,

        {{ clean_currency('slip_fee') }}                               as slip_fee,
        slip_fee                                                       as slip_fee_raw,

        _row_hash,
        _ingested_at,
        _batch_id

    from source

)

select
    *,
    case
        when customer_email is not null then 'email'
        when customer_phone is not null then 'phone'
        else 'unparseable'
    end                                                                as contact_type,
    (customer_email is not null)                                       as has_valid_email,
    (customer_phone is not null)                                       as has_valid_phone,
    (coalesce(customer_email, customer_phone, hull_id) is not null)     as is_resolvable_identity,
    {{ dbt.current_timestamp() }}                                      as dbt_loaded_at
from cleaned

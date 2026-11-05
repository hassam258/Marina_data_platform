{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='append_new_columns',
        partition_by=daily_partition('_ingested_at'),
        cluster_by=['sub_id'],
        tags=['raw', 'landing', 'pii']
    )
}}

/*
    Landing: Stripe events. Append-only, immutable, untyped.

    Billing events are immutable at source, so the hash anti-join is pure duplicate
    protection here rather than version tracking - re-reading the same event lands nothing.
    That makes this model safely re-runnable against an overlapping extract window.
*/

with raw_payload as (

    select
        sub_id,
        customer_email,
        plan_name,
        mrr_amount,
        event_type,
        event_timestamp,
        {{ landing_metadata([
            'sub_id', 'customer_email', 'plan_name',
            'mrr_amount', 'event_type', 'event_timestamp'
        ]) }}
    from {{ source('marina_raw', 'raw_stripe_subscriptions') }}

)

select * from raw_payload
where 1 = 1
{{ landing_dedupe_filter() }}

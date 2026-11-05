{{
    config(
        materialized='incremental',
        unique_key='sub_event_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        partition_by=daily_partition('event_timestamp_utc'),
        cluster_by=['sub_id'],
        tags=['staging', 'billing', 'pii'],
        post_hook="{{ apply_masking_policy('marina_email_mask', ['customer_email']) }}"
    )
}}

/*
    Staging: Stripe events. Grain: one row per subscription event. Append-only log.

    NO is_current_state FLAG HERE - deliberately. v1 computed it with
    row_number() over (partition by sub_id ...), which sorts the entire event log on
    every run. At a billion events that is the most expensive statement in the project.
   
    Source C has no status column, so subscription state is derived from event_type.
*/

with source as (

-- Billing events are immutable at source, so there are no versions to collapse here.
-- Duplicate re-reads within the replay window are caught by the landing hash; anything
-- outside it is absorbed by the merge on sub_event_id.
    select * from {{ ref('raw_stripe_subscriptions') }}
    where sub_id is not null
    {% if is_incremental() %}
      and {{ landing_watermark_filter('_ingested_at') }}
    {% endif %}

),

cleaned as (

    select
        trim(cast(sub_id as {{ dbt.type_string() }}))                    as sub_id,
        {{ normalize_email('customer_email') }}                          as customer_email,
        customer_email                                                   as customer_email_raw,
        nullif(trim(cast(plan_name as {{ dbt.type_string() }})), '')     as plan_name,
        lower(trim(cast(event_type as {{ dbt.type_string() }})))         as event_type,
        {{ clean_currency('mrr_amount') }}                               as mrr_amount,
        {{ to_utc_timestamp('event_timestamp') }}                        as event_timestamp_utc,
        _row_hash,
        _ingested_at,
        _batch_id
    from source

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['sub_id', 'event_type', 'event_timestamp_utc']) }} as sub_event_id,
        cleaned.*,
        case
            when lower(coalesce(plan_name, '')) like '%annual%'
              or lower(coalesce(plan_name, '')) like '%year%' then 'annual'
            else 'monthly'
        end                                                              as billing_interval,
        (event_type not in ('cancel', 'canceled', 'cancelled', 'cancellation', 'churn', 'delete'))
                                                                         as is_active_event,
        {% if var('annual_plan_amount_is_annual_total') %}
        case when lower(coalesce(plan_name, '')) like '%annual%'
             then round(mrr_amount / 12, 2) else mrr_amount end
        {% else %}
        mrr_amount
        {% endif %}                                                      as normalized_mrr_amount
    from cleaned

)

select * from final


    {{
    config(
        materialized='incremental',
        unique_key='sub_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        cluster_by=['is_active_event'],
        tags=['intermediate', 'billing']
    )
}}
    

/*
    Current state per subscription. Grain: one row per sub_id.

    This model exists so that nothing downstream ever sorts the full event log. On an
    incremental run it ranks only the events inside the lookback window, then merges - so
    the work is proportional to CHANGE, not to history. At a billion events that is the
    difference between a full sort and touching a few thousand rows.

    The guard against out-of-order arrival is explicit: a late event older than the stored
    state must not overwrite it.
*/

with events as (

    select * from {{ ref('stg_stripe_subscriptions') }}
    {% if is_incremental() %}
    where event_timestamp_utc >= {{ dbt.dateadd('day', -1 * var('billing_lookback_days'), dbt.current_timestamp()) }}
    {% endif %}

),

latest_in_window as (

    select
        sub_id,
        customer_email,
        plan_name,
        billing_interval,
        event_type,
        is_active_event,
        normalized_mrr_amount,
        event_timestamp_utc
    from events
    qualify row_number() over (
        partition by sub_id
        order by event_timestamp_utc desc, sub_event_id desc
    ) = 1

)

{% if is_incremental() %}

select l.*
from latest_in_window l
left join {{ this }} existing on l.sub_id = existing.sub_id
where existing.sub_id is null
   or l.event_timestamp_utc >= existing.event_timestamp_utc

{% else %}

select * from latest_in_window

{% endif %}

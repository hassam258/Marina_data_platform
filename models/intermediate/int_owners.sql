{{
    config(
        materialized='table',
        cluster_by=['owner_id'],
        tags=['intermediate', 'identity_resolution', 'pii'],
        post_hook=[
            "{{ apply_masking_policy('marina_name_mask', ['legal_name']) }}",
            "{{ apply_masking_policy('marina_email_mask', ['primary_email']) }}",
            "{{ apply_masking_policy('marina_phone_mask', ['primary_phone']) }}",
        ]
    )
}}

/*
    Conformed owner dimension. Grain: one row per owner_id.

    Survivorship, documented so support can explain any merge. Source A has NO timestamps,
    so recency is only available for billing records and the fallback signal is
    COMPLETENESS - the fuller value wins:

      primary_email / phone : most recent non-null, then longest
      legal_name            : non-null, PMS preferred (contract names beat self-entered
                              checkout names), then LONGEST - 'John Doe' over 'J. Doe'
      final tiebreak        : source_record_id, so the golden record never changes
                              between runs on identical input
*/

with xref as (

    select * from {{ ref('int_owner_identity_xref') }}

),

pms_attributes as (

    select booking_id, customer_full_name, marina_code
    from {{ ref('stg_marina_pms') }}

),

billing_attributes as (

    select sub_id, event_timestamp_utc
    from {{ ref('int_subscription_state') }}

),

enriched as (

    select
        x.owner_id,
        x.node_id,
        x.identity_source,
        x.source_record_id,
        x.email,
        x.phone,
        x.hull_id,
        p.customer_full_name    as full_name,
        p.marina_code,
        b.event_timestamp_utc   as observed_at_utc
    from xref x
    left join pms_attributes     p on x.identity_source = 'marina_pms' and x.source_record_id = p.booking_id
    left join billing_attributes b on x.identity_source = 'stripe'     and x.source_record_id = b.sub_id

),

ranked as (

    select
        *,
        row_number() over (
            partition by owner_id
            order by case when email is not null then 0 else 1 end,
                     observed_at_utc desc nulls last,
                     length(coalesce(email, '')) desc,
                     source_record_id
        ) as email_rank,
        row_number() over (
            partition by owner_id
            order by case when phone is not null then 0 else 1 end,
                     source_record_id
        ) as phone_rank,
        row_number() over (
            partition by owner_id
            order by case when full_name is not null then 0 else 1 end,
                     case when identity_source = 'marina_pms' then 0 else 1 end,
                     length(coalesce(full_name, '')) desc,
                     source_record_id
        ) as name_rank
    from enriched

)

select
    owner_id,
    max(case when name_rank  = 1 then full_name end)                    as legal_name,
    max(case when email_rank = 1 then email end)                        as primary_email,
    max(case when phone_rank = 1 then phone end)                        as primary_phone,
    max(case when name_rank  = 1 then marina_code end)                  as home_marina_code,

    count(*)                                                            as source_record_count,
    count(distinct identity_source)                                     as source_system_count,
    count(distinct email)                                               as distinct_email_count,
    count(distinct phone)                                               as distinct_phone_count,
    count(distinct hull_id)                                             as distinct_hull_count,

    max(case when identity_source = 'marina_pms' then 1 else 0 end) = 1 as exists_in_pms,
    max(case when identity_source = 'stripe'     then 1 else 0 end) = 1 as exists_in_stripe,
    (count(*) > 1)                                                      as is_merged_record,
    min(observed_at_utc)                                                as first_seen_at_utc,
    max(observed_at_utc)                                                as last_seen_at_utc
from ranked
group by owner_id

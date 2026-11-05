{{
    config(
        materialized='table',
        cluster_by=['key_id'],
        tags=['intermediate', 'identity_resolution', 'pii']
    )
}}

/*
    Edge list for identity resolution. Grain: one row per (node_id, key_id).

    NOT column-masked, unlike int_owner_identity_xref/int_owners. key_value mixes email,
    phone and hull values under one key_type-tagged column - a single Snowflake masking
    policy on that column can't apply email-shaped masking to some rows and hull-shaped
    masking to others. PII exposure here is bounded by schema access (intermediate is
    restricted) rather than column masking; see README > Security.

    A node is a source record; a key is a normalized contact value it carries. Two nodes
    sharing a key are connected. Keys are TYPE-PREFIXED ('email:', 'phone:', 'hull:') so a
    phone number can never collide with an unrelated field holding the same digits.

    ------------------------------------------------------------------------------
    THE FREQUENCY CAP - the most important twelve lines in this project
    ------------------------------------------------------------------------------
    Real contact data contains poisoned keys: the front-desk phone every walk-in booking
    gets typed with, noreply@marina.com, a QA test account. One such value appearing in
    500k rows makes 500k mutually-connected nodes, and label propagation dutifully merges
    them into ONE owner holding a large share of the customer base.

    It does not error. It produces a plausible table with a catastrophic row in it.

    Verified on the sample data plus 40 synthetic walk-ins sharing a front-desk number:
    uncapped, they were absorbed into a real customer's cluster (43 records). Capped, the
    bad edge is dropped and every genuine merge still happens.

    Poisoned keys are RETAINED here with a flag rather than filtered out, so the values
    polluting the source are visible in mrt_data_quality instead of vanishing.
*/

with pms_nodes as (

    select
        'marina_pms'                                   as identity_source,
        booking_id                                     as source_record_id,
        'marina_pms:' || booking_id                    as node_id,
        customer_email                                 as email,
        customer_phone                                 as phone,
        hull_id                                        as hull_id,
        customer_full_name                             as full_name,
        marina_code                                    as marina_code,
        cast(null as timestamp)                        as observed_at_utc
    from {{ ref('stg_marina_pms') }}

),

stripe_nodes as (

    select
        'stripe'                                       as identity_source,
        sub_id                                         as source_record_id,
        'stripe:' || sub_id                            as node_id,
        customer_email                                 as email,
        cast(null as {{ dbt.type_string() }})          as phone,
        cast(null as {{ dbt.type_string() }})          as hull_id,
        cast(null as {{ dbt.type_string() }})          as full_name,
        cast(null as {{ dbt.type_string() }})          as marina_code,
        event_timestamp_utc                            as observed_at_utc
    from {{ ref('int_subscription_state') }}

),

nodes as (

    select * from pms_nodes
    union all
    select * from stripe_nodes

),

keys_raw as (

    select node_id, 'email' as key_type, email   as key_value, 'email:' || email   as key_id
    from nodes where email is not null

    union all
    select node_id, 'phone', phone, 'phone:' || phone
    from nodes where phone is not null

    {% if var('enable_hull_bridging') %}
    union all
    select node_id, 'hull', hull_id, 'hull:' || hull_id
    from nodes where hull_id is not null
    {% endif %}

),

key_frequency as (

    select
        key_id,
        count(distinct node_id) as node_count
    from keys_raw
    group by key_id

)

select
    k.node_id,
    k.key_type,
    k.key_value,
    k.key_id,
    f.node_count                                       as key_node_count,
    (f.node_count > {{ var('max_nodes_per_key') }})     as is_poisoned_key
from keys_raw k
inner join key_frequency f on k.key_id = f.key_id

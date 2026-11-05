{{
    config(
        materialized='incremental',
        unique_key='node_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        full_refresh=is_weekly_full_refresh(),
        cluster_by=['owner_id'],
        tags=['intermediate', 'identity_resolution', 'pii'],
        post_hook=[
            "{{ apply_masking_policy('marina_email_mask', ['email']) }}",
            "{{ apply_masking_policy('marina_phone_mask', ['phone']) }}",
        ]
    )
}}

/*
    Record-level crosswalk: every source record -> its resolved owner_id.
    Consumed by int_owners, int_boats, the mart, and GDPR deletion.

    ------------------------------------------------------------------------------
    ALGORITHM - label propagation (connected components, bounded rounds)
    ------------------------------------------------------------------------------
        label_0    = the node's own id
        label_n+1  = MIN(label_n) across every node sharing a non-poisoned key
        owner_id   = MD5(final label)

    MIN() over node ids makes the winning label independent of scan order, so owner_id is
    stable across full refreshes. Unstable keys break every downstream join.

    Why three key types: because raw_contact holds only ONE identifier per row, two
    records for the same person can share no contact value at all.
        BK-901  John Doe   phone 5550192834   hull FL8812AB
        BK-902  J. Doe     jdoe_99@gmail.com  hull FL8812AB
        SUB-001            jdoe_99@gmail.com
    Email/phone alone leaves BK-901 with no edge and produces 3 owners where there are 2.
    The hull edge closes it.

    Cost: hull is a BOAT-level signal. A sold boat merges two different people, and
    Source A has no dates to sequence ownership with. Hence enable_hull_bridging and the
    has_ownership_history guard rail on int_boats.

    ------------------------------------------------------------------------------
    INCREMENTAL STRATEGY - the part that makes this viable at scale
    ------------------------------------------------------------------------------
    Resolution is globally coupled: one new record can merge two existing clusters. So a
    naive "process new rows only" is wrong. Instead:

      incremental run : resolve only the NEIGHBOURHOOD of changed nodes - the keys they
                        touch plus every node on those keys, seeded with those nodes'
                        EXISTING labels. Cheap, and correct for the common case.
      weekly run      : full_refresh=true rebuilds from scratch and heals any merge the
                        neighbourhood approach missed.

    Assuming incremental resolution is always exact is how identity tables silently rot.
    The weekly rebuild is the price of not assuming that.

    BEYOND ~50M IDENTITIES: replace the propagation CTEs with Spark GraphFrames or a
    dedicated matching service writing node_id -> owner_id back into this table. Every
    downstream contract is unchanged, which is the entire reason this model exists
    separately from int_owners.
*/

with all_keys as (

    select * from {{ ref('int_identity_keys') }}
    where not is_poisoned_key

),

{% if is_incremental() %}

changed_nodes as (

    select node_id from {{ ref('int_identity_keys') }}
    where node_id not in (select node_id from {{ this }})

),

affected_keys as (

    select distinct k.key_id
    from all_keys k
    inner join changed_nodes c on k.node_id = c.node_id

),

key_map as (

    select k.node_id, k.key_id
    from all_keys k
    inner join affected_keys a on k.key_id = a.key_id

),

seed_labels as (

    select
        km.node_id,
        coalesce(existing.owner_cluster_key, km.node_id) as label
    from (select distinct node_id from key_map) km
    left join {{ this }} existing on km.node_id = existing.node_id

),

labels_0 as (select node_id, label from seed_labels),

{% else %}

key_map as (select node_id, key_id from all_keys),

labels_0 as (select distinct node_id, node_id as label from all_keys),

{% endif %}

{% for i in range(1, var('identity_resolution_iterations') + 1) %}

key_labels_{{ i }} as (

    select km.key_id, min(l.label) as key_label
    from key_map km
    inner join labels_{{ i - 1 }} l on km.node_id = l.node_id
    group by km.key_id

),

labels_{{ i }} as (

    select
        l.node_id,
        least(l.label, coalesce(min(kl.key_label), l.label)) as label
    from labels_{{ i - 1 }} l
    left join key_map km on l.node_id = km.node_id
    left join key_labels_{{ i }} kl on km.key_id = kl.key_id
    group by l.node_id, l.label

),

{% endfor %}

node_attributes as (

    select
        node_id,
        max(case when key_type = 'email' then key_value end) as email,
        max(case when key_type = 'phone' then key_value end) as phone,
        max(case when key_type = 'hull'  then key_value end) as hull_id
    from {{ ref('int_identity_keys') }}
    group by node_id

),

resolved as (

    select
        l.node_id,
        l.label                                            as owner_cluster_key,
        {{ dbt.split_part('l.node_id', "':'", 1) }}        as identity_source,
        {{ dbt.split_part('l.node_id', "':'", 2) }}        as source_record_id,
        a.email,
        a.phone,
        a.hull_id
    from labels_{{ var('identity_resolution_iterations') }} l
    left join node_attributes a on l.node_id = a.node_id

)

select
    {{ dbt_utils.generate_surrogate_key(['owner_cluster_key']) }} as owner_id,
    owner_cluster_key,
    node_id,
    identity_source,
    source_record_id,
    email,
    phone,
    hull_id,
    case
        when email is not null   then 'email'
        when phone is not null   then 'phone'
        when hull_id is not null then 'hull_id'
        else 'unresolved'
    end                                                          as resolution_method,
    {{ dbt.current_timestamp() }}                                as resolved_at
from resolved

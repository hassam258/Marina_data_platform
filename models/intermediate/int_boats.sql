{{
    config(
        materialized='table',
        cluster_by=['owner_id'],
        tags=['intermediate', 'identity_resolution']
    )
}}

/*
    Conformed boat dimension. Grain: one row per standardized hull_id.

    hull_id is the only stable boat identifier available. Boat NAMES are neither unique
    nor stable - 'Sea Breeze' and 'Seabreeze' are the same hull, and two unrelated boats
    called 'Serenity' are not the same hull.

    NOTE: neither sample hull is a valid 12-character HIN (both are 8 chars), so these are
    internal registration codes. The length check is therefore set from profiled data, not
    from the ISO spec.
*/

with pms as (

    select * from {{ ref('stg_marina_pms') }}
    where hull_id is not null

),

xref as (

    select source_record_id as booking_id, owner_id
    from {{ ref('int_owner_identity_xref') }}
    where identity_source = 'marina_pms'

),

joined as (

    select
        p.hull_id,
        p.boat_name,
        p.slip_fee,
        p.marina_code,
        p.booking_id,
        x.owner_id
    from pms p
    left join xref x on p.booking_id = x.booking_id

),

ranked as (

    select
        *,
        row_number() over (
            partition by hull_id
            order by length(coalesce(boat_name, '')) desc, booking_id
        ) as completeness_rank
    from joined

)

select
    {{ dbt_utils.generate_surrogate_key(['hull_id']) }}      as boat_id,
    hull_id,
    max(case when completeness_rank = 1 then owner_id end)   as owner_id,
    max(case when completeness_rank = 1 then boat_name end)  as boat_name,
    max(case when completeness_rank = 1 then marina_code end) as marina_code,
    max(case when completeness_rank = 1 then slip_fee end)   as current_slip_fee,
    avg(slip_fee)                                            as avg_slip_fee,
    count(distinct booking_id)                               as booking_count,
    count(distinct owner_id)                                 as distinct_owner_count,
    count(distinct boat_name)                                as distinct_name_count,
    (count(distinct owner_id) > 1)                           as has_ownership_history,
    (count(distinct boat_name) > 1)                          as has_name_variants,
    (max(case when completeness_rank = 1 then owner_id end) is null) as is_orphan_boat,
    (length(hull_id) between 8 and 14)                       as has_plausible_hull_id
from ranked
group by hull_id

-- Guards the failure mode the frequency cap exists to prevent: a single owner absorbing
-- an implausible share of the customer base. This is the alarm if the cap is ever raised
-- too far or bypassed.
{{ config(severity='error') }}

with cluster_sizes as (
    select owner_id, count(*) as node_count
    from {{ ref('int_owner_identity_xref') }}
    group by owner_id
),
total as (select count(*) as total_nodes from {{ ref('int_owner_identity_xref') }})

select c.owner_id, c.node_count, t.total_nodes
from cluster_sizes c
cross join total t
where c.node_count > greatest(100, t.total_nodes * 0.01)

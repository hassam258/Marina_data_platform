-- Run before setting max_nodes_per_key. The right cap is a property of YOUR data: look at
-- the distribution and pick a value above the legitimate tail, below the defaults.
select
    key_type,
    key_node_count,
    count(*) as keys_at_this_frequency
from {{ ref('int_identity_keys') }}
group by key_type, key_node_count
order by key_node_count desc

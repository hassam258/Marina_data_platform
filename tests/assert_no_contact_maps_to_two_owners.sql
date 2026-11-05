-- Convergence check on label propagation: a non-poisoned contact value must resolve to
-- exactly one owner. A failure means the rounds did not converge, or the incremental
-- neighbourhood missed a merge that the weekly rebuild will heal.
select
    k.key_id,
    count(distinct x.owner_id) as owner_count
from {{ ref('int_identity_keys') }} k
join {{ ref('int_owner_identity_xref') }} x on k.node_id = x.node_id
where not k.is_poisoned_key
group by k.key_id
having count(distinct x.owner_id) > 1

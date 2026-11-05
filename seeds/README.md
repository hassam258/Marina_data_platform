# Seed provenance

`seed_vendor_service_map.csv` contains ONLY values observed in the source data, plus
casing variants of those same values. It is deliberately not padded with invented
service types: an exact-match join means invented rows never fire, so they would add
apparent coverage without adding any.

The real population procedure is a profiling query, not imagination:

```sql
select lower(trim(raw_service_type)) as raw_key,
       count(*) as job_count,
       count(distinct vendor_name) as vendor_count,
       min(created_at) as first_seen
from raw.marina_raw.raw_vendor_work_orders
group by 1
order by job_count desc;
```

Map the values covering ~95% of volume, let the tail fall to `'Other'`, and re-run
monthly against `mrt_unmapped_services`, which ranks exactly what is missing.

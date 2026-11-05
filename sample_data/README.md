# Sample data

The three case-study tables verbatim. `load_snowflake.sql` creates and populates them.

Expected after `dbt deps && dbt seed && dbt build`:

| Model | Rows | Note |
|---|---|---|
| `int_identity_keys` | 8 | 3 email + 1 phone + 4 hull edges, none poisoned |
| `int_owner_identity_xref` | 5 | 3 PMS bookings + 2 subscriptions |
| `int_owners` | 2 | BK-901 + BK-902 + SUB-001 resolve together; BK-903 + SUB-002 together |
| `int_boats` | 2 | `FL8812AB` (Sea Breeze), `CA9910XC` (Terminator II) |
| `int_vendor_jobs_daily` | 3 | one row per job date |
| MRR / ARR | 3,700 / 44,400 | see the amount ambiguity in the root README |
| Avg turnaround | 4.75 h | WO-101 6.5h, WO-102 3.0h; WO-103 excluded |

The owner merge is the load-bearing result: `John Doe` (phone only) and `J. Doe`
(email only) share no contact value and are linked only through `hull_id`.

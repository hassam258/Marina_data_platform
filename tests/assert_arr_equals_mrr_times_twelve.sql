-- ARR must be exactly 12x MRR on every snapshot date. Catches a broken unpivot or a
-- silently changed revenue definition before the board sees it.
with pivoted as (
    select
        metric_date,
        max(case when metric_name = 'monthly_recurring_revenue' then metric_value end) as mrr,
        max(case when metric_name = 'annual_recurring_revenue'  then metric_value end) as arr
    from {{ ref('mrt_executive_kpis') }}
    where metric_grain = 'company'
    group by metric_date
)
select * from pivoted
where abs(coalesce(arr, 0) - coalesce(mrr, 0) * 12) > 0.01

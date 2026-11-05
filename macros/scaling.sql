{#
    Scaling helpers. Every incremental model in this project uses these, so the
    windowing policy is defined once rather than copy-pasted into ten models.
#}

{% macro incremental_window(timestamp_column, lookback_var) -%}
    {#-
        Rolling-window predicate. NOT `> max(ts)`: a high-watermark silently drops every
        late-arriving row, and vendor paperwork is routinely days late. The window is
        reprocessed and merged, so reprocessing is idempotent rather than duplicating.
    -#}
    {{ timestamp_column }} >= coalesce(
        (
            select {{ dbt.dateadd('day', -1 * var(lookback_var), 'max(' ~ timestamp_column ~ ')') }}
            from {{ this }}
        ),
        cast('1900-01-01' as timestamp)
    )
{%- endmacro %}


{% macro merge_predicate(timestamp_column, lookback_var) -%}
    {#-
        Passed as incremental_predicates so the MERGE only scans the target partitions the
        window can touch. Without this the merge scans the whole destination table - at a
        billion rows that is the difference between seconds and hours.
    -#}
    DBT_INTERNAL_DEST.{{ timestamp_column }} >= {{ dbt.dateadd('day', -1 * (var(lookback_var) + 1), dbt.current_timestamp()) }}
{%- endmacro %}


{% macro daily_partition(field) -%}
    {#- Daily partitions on BigQuery; Snowflake prunes on natural load order + clustering. -#}
    {{ return({'field': field, 'data_type': 'timestamp', 'granularity': 'day'} if target.type == 'bigquery' else none) }}
{%- endmacro %}


{% macro is_weekly_full_refresh() -%}
    {#-
        Incremental identity resolution is correct for the common case but can miss a
        merge that spans beyond the affected neighbourhood. A scheduled full rebuild heals
        it. Cheap to reason about, and the alternative (assuming incremental is always
        exact) is how identity tables silently rot.
    -#}
    {{ return(run_started_at.weekday() == var('identity_full_refresh_weekday')) }}
{%- endmacro %}


{% macro ingestion_watermark_filter() -%}
    {%- if var('enable_ingestion_watermark') -%}
        and {{ var('ingestion_watermark_column') }} >= coalesce(
            (select max({{ var('ingestion_watermark_column') }}) from {{ this }}),
            cast('1900-01-01' as timestamp)
        )
    {%- endif -%}
{%- endmacro %}


{% macro landing_watermark_filter(timestamp_column='_ingested_at') -%}
    {#-
        Staging processes what newly LANDED, not what recently happened. This is the whole
        reason the landing layer exists: a work order filed three weeks after the job still
        gets picked up, because the filter is on arrival time rather than business time.
        `>=` rather than `>` so a run interrupted mid-batch reprocesses the boundary; the
        merge makes that idempotent.
    -#}
    {{ timestamp_column }} >= coalesce(
        (select max({{ timestamp_column }}) from {{ this }}),
        cast('1900-01-01' as timestamp)
    )
{%- endmacro %}

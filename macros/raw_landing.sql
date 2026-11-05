{#
    Landing-layer helpers.

    The raw layer is APPEND-ONLY and IMMUTABLE. Nothing is cleaned, cast, renamed or
    filtered. Its only job is to record exactly what arrived, when, and in which batch, so
    that every downstream layer can be rebuilt from warehouse-local data instead of
    re-pulling from a source system that may no longer hold the same rows.

    SCALE CAVEAT, stated plainly: at billions of rows the landing itself belongs to the
    loader (Fivetran / Kafka -> Iceberg / Snowpipe), not to dbt. Modelling it here makes
    the contract explicit and gives replay on the sample data; in production these three
    models become external tables over the loader's output and the same columns are stamped
    by the pipeline. The layer stays, the engine changes.
#}

{% macro landing_metadata(source_columns) -%}
    {#-
        Ingestion stamps. _row_hash over the full source payload is the idempotency key:
        re-reading the same row produces the same hash and is skipped, while a CHANGED row
        produces a new hash and lands as a new version. The landing table therefore holds
        full change history and staging picks the latest version per key.
    -#}
    {{ dbt_utils.generate_surrogate_key(source_columns) }}  as _row_hash,
    {{ dbt.current_timestamp() }}                           as _ingested_at,
    cast({{ dbt.current_timestamp() }} as date)             as _ingestion_date,
    '{{ invocation_id }}'                                   as _batch_id
{%- endmacro %}


{% macro landing_dedupe_filter() -%}
    {#-
        Anti-join against recently landed hashes only. An unbounded NOT EXISTS against a
        billion-row landing table would scan the whole thing on every run; bounding it to
        the replay window keeps the scan proportional to recent volume.

        Trade-off: a row that reappears unchanged after the window re-lands as a duplicate
        version. Harmless - staging dedupes by key and keeps the latest - and far cheaper
        than a full-history scan.
    -#}
    {% if is_incremental() %}
    and not exists (
        select 1
        from {{ this }} landed
        where landed._row_hash = raw_payload._row_hash
          and landed._ingestion_date >= cast(
                {{ dbt.dateadd('day', -1 * var('landing_replay_days'), dbt.current_timestamp()) }} as date)
    )
    {% endif %}
{%- endmacro %}

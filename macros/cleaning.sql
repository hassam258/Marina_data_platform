{#
    Cleaning macros - adapter-aware (Snowflake + BigQuery).

    SCALE NOTE. At billions of rows these regexes should not run in dbt at all: cleaning
    moves into the ingestion job (Spark/Flink) and staging becomes rename-and-type only.
    The logic is identical either way, which is why it lives in macros - the same
    definitions port to the ingest layer without being rewritten from the SQL.
#}

{% macro clean_currency(column_name) -%}
    cast(
        nullif(regexp_replace(cast({{ column_name }} as {{ dbt.type_string() }}), '[^0-9.-]', ''), '')
        as decimal(18, 2)
    )
{%- endmacro %}


{% macro normalize_email(column_name) -%}
    {%- set pattern = '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}' -%}
    {%- if target.type == 'bigquery' -%}
        lower(nullif(trim(regexp_extract(cast({{ column_name }} as string), r'{{ pattern }}')), ''))
    {%- else -%}
        lower(nullif(trim(regexp_substr(cast({{ column_name }} as {{ dbt.type_string() }}), '{{ pattern }}')), ''))
    {%- endif -%}
{%- endmacro %}


{% macro normalize_phone(column_name) -%}
    {#-
        Digits only, keep the national significant number. Values below
        min_phone_digits are junk, not identity keys - and critically they become NULL,
        not '', because NULL never equals NULL. An empty string would match every other
        empty string and merge every contactless record into one owner.
    -#}
    {%- set digits %}
        regexp_replace(cast({{ column_name }} as {{ dbt.type_string() }}), '[^0-9]', '')
    {%- endset %}
    case
        when length({{ digits }}) between {{ var('min_phone_digits') }} and 15
            then right({{ digits }}, 10)
        else null
    end
{%- endmacro %}


{% macro clean_name(column_name) -%}
    nullif(initcap(trim(regexp_replace(cast({{ column_name }} as {{ dbt.type_string() }}), '\\s+', ' '))), '')
{%- endmacro %}


{% macro clean_hull_id(column_name) -%}
    nullif(upper(regexp_replace(cast({{ column_name }} as {{ dbt.type_string() }}), '[^A-Za-z0-9]', '')), '')
{%- endmacro %}


{% macro to_utc_timestamp(column_name) -%}
    {#-
        CAVEAT: source timestamps are naive - no offset, no timezone column. This treats
        them as already-UTC. For a platform "scaling to new markets" that assumption is
        wrong and needs a source-timezone column. Flagged in the README, not hidden here.
    -#}
    {%- if target.type == 'bigquery' -%}
        coalesce(
            safe_cast({{ column_name }} as timestamp),
            safe.parse_timestamp('%Y-%m-%d %H:%M:%E*S', cast({{ column_name }} as string)),
            safe.parse_timestamp('%d/%m/%Y %H:%M', cast({{ column_name }} as string))
        )
    {%- else -%}
        coalesce(
            try_to_timestamp_ntz(cast({{ column_name }} as varchar)),
            try_to_timestamp_ntz(cast({{ column_name }} as varchar), 'DD/MM/YYYY HH24:MI')
        )
    {%- endif -%}
{%- endmacro %}


{% macro hours_between(start_column, end_column) -%}
    cast({{ dbt.datediff(start_column, end_column, 'second') }} / 3600.0 as decimal(18, 4))
{%- endmacro %}

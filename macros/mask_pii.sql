{% macro hash_pii(column_name, salt_env_var='PII_HASH_SALT') -%}
    case when {{ column_name }} is null then null
         else {{ dbt.hash(column_name ~ " || '" ~ env_var(salt_env_var, 'local-dev-salt') ~ "'") }} end
{%- endmacro %}

{% macro mask_email(column_name) -%}
    case when {{ column_name }} is null then null
         else left({{ column_name }}, 1) || '***@' || {{ dbt.split_part(column_name, "'@'", 2) }} end
{%- endmacro %}

{% macro mask_phone(column_name) -%}
    case when {{ column_name }} is null then null else '******' || right({{ column_name }}, 4) end
{%- endmacro %}

{% macro apply_masking_policy(policy_name, columns) -%}
    {%- if target.type == 'snowflake' and target.name == 'prod' -%}
        {%- for column in columns -%}
            alter table {{ this }} modify column {{ column }} set masking policy {{ policy_name }} force;
        {%- endfor -%}
    {%- endif -%}
{%- endmacro %}

{#
    Creates the policy OBJECTS that apply_masking_policy() binds to columns. Split into
    three policies, not one generic one, because the three PII types need different
    masking logic (a name isn't masked the same way as an email) - see
    models/staging/schema.yml for the masking_strategy this mirrors per column.

    Unmasked visibility is role-gated via IS_ROLE_IN_SESSION() rather than a hardcoded
    role check inline in every query - one var controls who sees clear text.

    Wired as an on-run-start hook (dbt_project.yml) so the policy exists before any
    model's post-hook tries to bind it. Gated to prod Snowflake, same as
    apply_masking_policy() itself, so this is a no-op in dev and on BigQuery.
#}
{% macro create_pii_masking_policies() -%}
    {%- if target.type == 'snowflake' and target.name == 'prod' -%}
        {%- set unmasked_role = var('pii_unmasked_role', 'PII_VIEWER') -%}

        create masking policy if not exists marina_email_mask as (val string) returns string ->
            case when is_role_in_session('{{ unmasked_role }}') then val
                 else {{ mask_email('val') }} end;

        create masking policy if not exists marina_phone_mask as (val string) returns string ->
            case when is_role_in_session('{{ unmasked_role }}') then val
                 else {{ mask_phone('val') }} end;

        create masking policy if not exists marina_name_mask as (val string) returns string ->
            case when is_role_in_session('{{ unmasked_role }}') then val
                 when val is null then null
                 else left(val, 1) || repeat('*', greatest(length(val) - 1, 0)) end;
    {%- endif -%}
{%- endmacro %}

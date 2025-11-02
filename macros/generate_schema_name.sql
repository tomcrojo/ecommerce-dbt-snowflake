{% macro generate_schema_name(custom_schema_name, node) %}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- elif target.name == 'dev' -%}

        {# Dev environment: prefix custom schema with 'dev_' for isolation #}
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- elif target.name == 'staging' -%}

        {# Staging environment: prefix custom schema with 'stg_' #}
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- else -%}

        {# Production: use custom schema directly (clean names for BI) #}
        {{ custom_schema_name | trim }}

    {%- endif -%}

{% endmacro %}

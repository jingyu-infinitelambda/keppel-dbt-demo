{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    
    {# no custom schema configured for the model; use target configuration #}
    {%- if custom_schema_name is none -%}

      {% if target.name == "prod" %}
            {{ default_schema }}

      {# add DEV_ prefix to default schema #}
      {%- else -%}
            {{ "DEV_" ~ default_schema }}
      {% endif %}

    {# custom schema configured for the model either in config block or dbt_project.yml #}
    {%- else -%}

      {% if target.name == 'prod' %}
            {{ custom_schema_name | trim }}

      {# add DEV_ prefix to custom schema #}
      {%- else -%}
            {{ "DEV_" ~  custom_schema_name | trim }}
      {% endif %}
    {% endif %}

{%- endmacro %}

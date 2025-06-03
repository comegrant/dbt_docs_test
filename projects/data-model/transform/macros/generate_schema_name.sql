{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if target.name != 'local_dev' and target.name != 'prdev' and custom_schema_name is not none -%}

        {{ custom_schema_name | trim }}
    
    {%- elif target.name == 'local_dev' and custom_schema_name is not none -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- elif target.name == 'prdev' and custom_schema_name is not none -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}
    
    {%- else -%}

        {{ default_schema }}

    {%- endif -%}

{%- endmacro %}
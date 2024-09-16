{% macro generate_silver_model_sql(source_name, source_table_name, leading_commas=True) %}

{%- set source_relation = source(source_name, source_table_name) -%}

{%- set columns = adapter.get_columns_in_relation(source_relation) -%}
{% set column_names=columns | map(attribute='name') %}
{% set base_model_sql -%}

with 

source as (

    select * from {% raw %}{{ source({% endraw %}'{{ source_name }}', '{{ source_table_name }}'{% raw %}) }}{% endraw %}

)

, renamed as (

    select

        {% raw %}
        {# ids #}
        -- place ids here

        {# strings #}
        -- place strings here

        {# numerics #}
        -- place numerics here
        
        {# booleans #}
        -- place booleans here
        
        {# date #}
        -- place dates here
        
        {# timestamp #}
        -- place timestamps here
        
        {# scd #}
        -- place slowly change dimension fields here
        
        {# system #}
        -- place system columns here
        {% endraw %}

        {%- for column in column_names %}
        {{", " if not loop.first}}{{ column | lower }}
        {%- endfor %}

    from source

)

select * from renamed

{%- endset %}

{% if execute %}

    {{ print(base_model_sql) }}
    {% do return(base_model_sql) %}

{% endif %}

{% endmacro %}

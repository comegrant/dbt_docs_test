{% macro get_scd_valid_to(column_timestamp=None, column_id=None) %}
    
    {%- if column_timestamp is none and column_id is none -%}

        cast('{{ var("future_proof_date") }}' as timestamp)
    
    {%- elif column_timestamp is not none and column_id is none -%}

        coalesce({{ column_timestamp }}, cast('{{ var("future_proof_date") }}' as timestamp))
    
    {%- else -%}

        coalesce(
            lead({{ column_timestamp }}, 1) over (partition by {{ column_id }} order by {{ column_timestamp }})
            , cast('{{ var("future_proof_date") }}' as timestamp)
        )

    {%- endif -%}

{% endmacro %}

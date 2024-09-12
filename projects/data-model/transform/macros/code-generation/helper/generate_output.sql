{% macro generate_output(output) %}
    {% if execute %}
        {% set final_output = output | join('\n') %}
        {{ print(final_output) }}
        {% do return(final_output) %}
    {% endif %}
{% endmacro %}

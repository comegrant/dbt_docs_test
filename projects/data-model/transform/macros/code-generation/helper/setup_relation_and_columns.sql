{% macro setup_relation_and_columns(model_name) %}
    {% set relation = ref(model_name) %}
    {% set columns_info = adapter.get_columns_in_relation(relation) %}
    {% set output = [] %}
    {% do return({'columns_info': columns_info, 'output': output}) %}
{% endmacro %}

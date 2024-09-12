{% macro generate_data_test_yaml(column_name, column_type) %}
    {% set tests = '' %}

    {% if '_id' in column_name or 'pk' in column_name %}
        {% set tests = 
            "          - unique\n" ~
            "          - not_null" 
        %}
        
    {% elif 'string' == column_type %}
        {% set tests = 
            "          - not_null\n" ~
            "          - accepted_values:\n" ~
            "              values: []" 
            "              config:" 
            "                severity: warn" 
        %}
        
    {% elif 'fk' in column_name %}
        {% set fk_column = column_name.replace('fk_', '') %}
        {% set pk_column = fk_column.replace('fk', 'pk') %}
        {% set tests = 
            "          - unique\n" ~
            "          - not_null\n" ~
            "          - relationships:\n" ~
            "              to: " ~ ref(fk_column) ~ "\n" ~
            "              field: " ~ pk_column
        %}
        
    {% else %}
        {% set tests = "          - not_null" %}
    {% endif %}

    {{ return(tests) }}
{% endmacro %}

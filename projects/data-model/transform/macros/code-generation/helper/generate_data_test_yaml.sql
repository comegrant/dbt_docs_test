{% macro generate_data_test_yaml(column_name, column_type) %}
    {% set tests = '' %}

    {% if 'pk_' in column_name %}
        {% set tests = 
            "        constraints:\n" ~
            "          - type: primary_key\n" ~ 
            "          - type: not_null\n" ~ 
            "        data_tests:\n" ~
            "          - unique\n" ~
            "          - not_null" 
        %}

    {% elif '_id' in column_name %}
        {% set tests = 
            "        data_tests:\n" ~
            "          - unique\n" ~
            "          - not_null" 
        %}

    {% elif 'fk_' in column_name %}
        {% set related_model = column_name.replace('fk_', '') %}
        {% set pk_column = column_name.replace('fk', 'pk') %}
        {% set tests =
            "        constraints:\n" ~
            "          - type: foreign_key\n" ~ 
            "            to: ref('" ~ related_model ~ "')\n" ~
            "            to_columns: [" ~ pk_column ~ "]\n" ~
            "        data_tests:\n" ~
            "          - not_null\n" ~
            "          - relationships:\n" ~
            "              to: ref('" ~ related_model ~ "')\n" ~
            "              field: " ~ pk_column
        %}

    {% elif 'string' == column_type %}
        {% set tests =
            "        data_tests:\n" ~
            "          - not_null\n" ~
            "          - accepted_values:\n" ~
            "              values: []\n" 
            "              config:\n" 
            "                severity: warn" 
        %}
        
    {% else %}
        {% set tests = 
            "        data_tests:\n" ~
            "          - not_null" %}
    {% endif %}

    {{ return(tests) }}
{% endmacro %}

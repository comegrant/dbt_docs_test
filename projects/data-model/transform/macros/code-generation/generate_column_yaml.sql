{% macro generate_column_yaml(model_name) %}
    {% set setup = setup_relation_and_columns(model_name) %}
    {% set columns_info = setup.columns_info %}
    {% set output = setup.output %}

    {% do output.append("    columns:") %}

    {% for column in columns_info %}
        {% set column_info = 
            "      - name: " ~ column.name ~ 
            "\n        data_type: " ~ column.dtype ~ 
            "\n        description: \"{{ doc('column__" ~ column.name ~ "') }}\"" 
            %}
        {% do output.append(column_info) %}
        {% do output.append('        data_tests:') %}
        {% set data_tests = generate_data_test_yaml(column.name, column.dtype) %}
        {% do output.append(data_tests) %}
        {% do output.append('') %}
    {% endfor %}

    {% do generate_output(output) %}
{% endmacro %}
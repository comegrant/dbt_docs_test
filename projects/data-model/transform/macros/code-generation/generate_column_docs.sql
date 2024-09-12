{% macro generate_column_docs(model_name) %}
    {% set setup = setup_relation_and_columns(model_name) %}
    {% set columns_info = setup.columns_info %}
    {% set output = setup.output %}

    {% for column in columns_info %}
        {% if 'source_' not in column.name %}
            {% set doc_header = "{% docs column__" ~ column.name ~ " %}" %}
            {% do output.append(doc_header) %}
            {% do output.append('') %}
            {% do output.append('...') %}
            {% do output.append('') %}
            {% set end_docs = "{% enddocs %}" %}
            {% do output.append(end_docs) %}
            {% do output.append('') %}
        {% endif %}
    {% endfor %}

    {% do generate_output(output) %}
{% endmacro %}
{#
    Get a list of all tables that exist in a schema in Databricks, but does not exist in the dbt project.
    This can be useful to identify tables that most likely can be dropped.
    In the command line, run:
    `dbt run-operation find_extra_tables_in_databricks --args '{"schema": "your_schema_name"}' --target your_target_name`
#}

{% macro find_extra_tables_in_databricks(schema) %}

    {% set databricks_tables_query %}
        SHOW TABLES IN {{ schema }}
    {% endset %}
    {% set databricks_tables = run_query(databricks_tables_query).rows %}
    {% set databricks_table_names = databricks_tables | map(attribute=1) | list %}

    {% set dbt_models = [] %}
    {% for node in graph.nodes.values() %} 
        {% if (node.resource_type == 'model' or node.resource_type == 'snapshot') and node.schema == schema %}
            {% do dbt_models.append(node.name) %}
        {% endif %}
    {% endfor %}

    {% set extra_tables = [] %}
    {% for table in databricks_table_names %}
        {% if table not in dbt_models %}
            {% do extra_tables.append(table) %}
        {% endif %}
    {% endfor %}

    {% if extra_tables %}
        {{ log("Tables in Databricks, but not in dbt: ", info=True) }}
        {% for table in extra_tables %}
            {{ log(table, info=True) }}
        {% endfor %}
    {% else %}
        {{ log("No extra tables found.", info=True) }}
    {% endif %}
{% endmacro %}

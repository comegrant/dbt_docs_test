{% macro drop_tables_in_personal_schemas(dry_run=True) %}
    {% set schema_prefix = target.schema %}

    {% set databases = run_query("SHOW DATABASES") %}

    {% for row in databases %}
        {% set schema_name = row[0] %}
        
        {% if schema_name.startswith(schema_prefix) %}
            {% do log("Deleting tables and views in schema: " ~ schema_name, info=True) %}
            {% do drop_tables_in_schema(schema_name=schema_name, dry_run=dry_run) %}
        {% endif %}
    {% endfor %}

{% endmacro %}

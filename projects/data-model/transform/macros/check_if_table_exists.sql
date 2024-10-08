{% macro table_exists(relation) %}
    {% if adapter.get_relation(database=relation.database, schema=relation.schema, identifier=relation.identifier) %}
        {{ return(true) }}
    {% else %}
        {{ return(false) }}
    {% endif %}
{% endmacro %}

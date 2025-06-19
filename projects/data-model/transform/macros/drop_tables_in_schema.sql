{% macro drop_tables_in_schema(schema_name,dry_run=True) %}
   
    {% set views_query = "SHOW VIEWS IN `" ~ schema_name ~ "`" %}
    {% set views = run_query(views_query) %}
    {% set tables_query = "SHOW TABLES IN `" ~ schema_name ~ "`" %}
    {% set tables = run_query(tables_query) %}
    
    {% if execute %}
        {% if views %}
            {% do log("The following views were found in schema " ~ schema_name ~ ":", info=True) %}
            {% for row in views %}
                {% set view_name = row[1] %}
                {% do log("- " ~ view_name, info=True) %}
            {% endfor %}
            
            {% if dry_run %}
                {% do log("This is a dry run. No views will be dropped.", info=True) %}
            {% else %}
                {% do log("Starting dropping views...", info=True) %}
                {% for row in views %}
                    {% set view_name = row[1] %}
                    {% do log("Dropping view: " ~ view_name, info=True) %}
                    {% do run_query("DROP VIEW IF EXISTS `" ~ schema_name ~ "`.`" ~ view_name ~ "`") %}
                {% endfor %}
            {% endif %}
        {% else %}
            {% do log("No views found in schema " ~ schema_name ~ ".", info=True) %}
        {% endif %}

        {% if tables %}
            {% do log("The following tables were found in schema " ~ schema_name ~ ":", info=True) %}
            {% for row in tables %}
                {% set table_name = row[1] %}
                {% do log("- " ~ table_name, info=True) %}
            {% endfor %}
            
            {% if dry_run %}
                {% do log("This is a dry run. No tables will be dropped.", info=True) %}
            {% else %}
                {% do log("Starting dropping tables...", info=True) %}
                {% for row in tables %}
                    {% set table_name = row[1] %}
                    {% do log("Dropping table: " ~ table_name, info=True) %}
                    {% do run_query("DROP TABLE IF EXISTS `" ~ schema_name ~ "`.`" ~ table_name ~ "`") %}
                {% endfor %}
            {% endif %}
        {% else %}
            {% do log("No tables found in schema " ~ schema_name ~ ".", info=True) %}
        {% endif %}
    {% endif %}
{% endmacro %}

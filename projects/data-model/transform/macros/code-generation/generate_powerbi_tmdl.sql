{% macro generate_powerbi_tmdl(model_name) %}
    {% set setup = setup_relation_and_columns(model_name) %}
    {% set columns_info = setup.columns_info %}
    {% set output = setup.output %}

    {# Determine Table Name #}
    {% if 'fact' in model_name %}
        {% set table_name = model_name.split('_', 1)[-1].replace('_', ' ').title().rstrip('s') ~ ' Measures' %}
    {% elif 'bridge' in model_name %}
        {% set table_name = model_name.replace('_', ' ').title() %}
    {% else %}
        {% set table_name = model_name.split('_', 1)[-1].replace('_', ' ').title().strip() %}
    {% endif %}

    {# Table Header #}
    {% do output.append('') %}
    {% do output.append('/// Write table end user friendly description here (can copy from Databricks if relevant)') %}
    {% do output.append('table ' ~ "'" ~ table_name ~ "'") %}
    {% do output.append('    lineageTag: ' ~ table_name.replace(' ', '-') ~'-Table') %}
    {% do output.append('') %}

    {# Iterate Over Columns #}
    {% for column in columns_info %}
        
        {# Column Name Formatting #}
        {% if 'fk' not in column.name and 'pk' not in column.name and 'id' not in column.name %}
            {% set column_name = "'" ~ column.name.replace('_', ' ').replace('name', '').title().strip() ~ "'" %}
        {% else %}
            {% set column_name = column.name %}
        {% endif %}

        {# Determine Data Type and Format String #}
        {% if 'string' in column.dtype or 'char' in column.dtype %}
            {% set data_type = 'string' %}
            {% set format_string = '' %}
        {% elif 'int' in column.dtype %}
            {% set data_type = 'int64' %}
            {% set format_string = 'formatString: 0' %}
        {% elif 'decimal' in column.dtype %}
            {% set data_type = 'double' %}
            {% set format_string = '' %}
        {% elif 'boolean' in column.dtype %}
            {% set data_type = 'boolean' %}
            {% set format_string = 'formatString: """TRUE"";""TRUE"";""FALSE"""' %}
        {% elif 'timestamp' in column.dtype %}
            {% set data_type = 'dateTime' %}
            {% set format_string = 'formatString: General Date' %}
        {% elif 'date' in column.dtype %}
            {% set data_type = 'dateTime' %}
            {% set format_string = 'formatString: Short Date' %}
        {% else %}
            {% set data_type = 'unknown, please fill in' %}
            {% set format_string = '' %}
        {% endif %}

        {# Append Column Details #}
        {% do output.extend([
            '    /// Write user friendly description (can copy from Databricks if relevant)',
            '    column ' ~ column_name,
            '        dataType: ' ~ data_type,
            '        isHidden',
            '        isAvailableInMdx: false'
        ]) %}

        {% if format_string %}
            {% do output.append('        ' ~ format_string) %}
        {% endif %}

        {% do output.extend([
            '        summarizeBy: none',
            '        sourceColumn: ' ~ column.name,
            ''
        ]) %}
    {% endfor %}

    {# Table Footer #}
    {% do output.extend([
        "    partition '" ~ table_name ~ "' = m",
        '        mode: directQuery',
        '        source =',
        '                let',
        '                    Source = Databricks.Catalogs(Host, HttpPath, [Catalog=null, Database=null, EnableAutomaticProxyDiscovery=null]),',
        '                    catalog_Database = Source{[Name=Catalog,Kind="Database"]}[Data],',
        '                    gold_Schema = catalog_Database{[Name=Schema,Kind="Schema"]}[Data],',
        '                    ' ~ model_name ~ '_Table = gold_Schema{[Name="' ~ model_name ~ '",Kind="Table"]}[Data]',
        '                in',
        '                    ' ~ model_name ~ '_Table'
    ]) %}

    {% do generate_output(output) %}
{% endmacro %}

{% test required_values(model, column_name, value_list) %}

    {% set where_clause = config.get('where', '1=1') %}
    {% set formatted_value_list = [] %}
    {% for value in value_list %}
        {% do formatted_value_list.append("'" ~ value ~ "'") %}
    {% endfor %}

    with filtered_rows as (
        select *
        from {{ model }}
        where {{ where_clause }}
    )
    
    , values_to_check as (
        select explode(array({{ formatted_value_list | join(', ') }})) as expected_value
    )

    , existing_values as (
        select distinct {{ column_name }} as actual_value
        from filtered_rows
    )

    , missing_values as (
        select values_to_check.expected_value
        from values_to_check
        left join existing_values
        on values_to_check.expected_value = existing_values.actual_value
        where existing_values.actual_value is null
    )

    select * from missing_values

{% endtest %}

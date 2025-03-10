-- Checks if the values in the column where the test is added is equal to the values in the column specified in the value field.
-- It also allows for a where clause to be added to the query.
-- If all values are equal, the test will pass, otherwise it will fail.
{% test equal_to_column(model, column_name, value) %}

    {% set where_clause = config.get('where', '1=1') %}

    select * from {{ model }}
    where  {{ where_clause }}
    and {{ column_name }} != {{ value }}

{% endtest %}

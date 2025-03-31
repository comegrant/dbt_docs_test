-- Checks if the ranking of rows is the same across columns
-- when partitioning by the same id.
-- Can for instance be used to check that valid_from and valid_to 
-- dates have the same rank per the column id.
{% test consistent_row_ranking(model, id_column, order_by_columns) %}
    with ranked as (
        select
            {{ id_column }},
            {% for col in order_by_columns %}
                row_number() over (
                    partition by {{ id_column }}
                    order by {{ col }}
                ) as rn_{{ col }}{% if not loop.last %},{% endif %}
            {% endfor %}
        from {{ model }}
    ),
    
    inconsistencies as (
        select *
        from ranked
        where
            {% for col in order_by_columns %}
                rn_{{ col }} != rn_{{ order_by_columns[0] }}{% if not loop.last %} or{% endif %}
            {% endfor %}
    )

    select * from inconsistencies

{% endtest %}

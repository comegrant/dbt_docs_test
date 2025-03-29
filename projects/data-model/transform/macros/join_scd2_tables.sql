-- assumes the valid_from and valid_to columns are called valid_from and valid_to
-- assumes all tables has filled valid_from and valid_to and no null values
-- assumes the id column are called the same in all tables
{% macro join_scd2_tables(id_column, table_names) %}

with

    unified_timeline as (
        {% for table in table_names %}
        select
            {{ id_column }}
            , valid_from
        from {{ table }}
        {% if not loop.last %}
        union
        {% endif %}
        {% endfor %}
    )

    , unified_timeline_recalculate_valid_to as (
        select
            {{ id_column }}
            , valid_from
            , {{ get_scd_valid_to('valid_from', id_column) }} as valid_to
        from unified_timeline
        order by 1, 2, 3
    )


select
    timeline.*
    , case
        when timeline.valid_to = '{{ var("future_proof_date") }}'
        then true
        else false
    end as is_current
    {% for table in table_names %}
            , {{ table }}.* except({{id_column}}, valid_from, valid_to)
    {% endfor %}
from unified_timeline_recalculate_valid_to timeline
-- Join all tables with specified conditions
{% for table in table_names %}
left join {{ table }}
    on timeline.{{ id_column }} = {{ table }}.{{ id_column }}
    and timeline.valid_from >= {{ table }}.valid_from
    and timeline.valid_to <= {{ table }}.valid_to
{% endfor %}


{% endmacro %}

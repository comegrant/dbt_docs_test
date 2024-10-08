-- assumes the valid_from and valid_to columns in both tables are called valid_from and valid_to
-- assumes both tables has filled valid_from and valid_to and no null values
-- assumes the joining id (join_id) that is used to join the tables are called the same in both tables
-- assumes we have replaced instances of valid_to = null with a future_proof_date = '9999-01-01'.
-- table2_column is a column from the second table that will be a part of the final table. If more columns from this table needs to be included it can be done at a later stage.
{% macro join_snapshots(table1, table2, join_id, table2_column) %}

with
    unified_timeline as (
        select {{join_id}}, valid_from from {{table1}}
        union
        select {{join_id}}, valid_from from {{table2}}
    )

    , timeline as (
        select
            {{join_id}},
            valid_from,
            {{ get_scd_valid_to('valid_from',  join_id) }} as valid_to
        from unified_timeline
    )

    , timeline_and_tables_joined as (
        select
            timeline.{{join_id}}
            , {{table1}}.* except({{join_id}}, valid_from, valid_to)
            , {{table2}}.{{table2_column}}
            , timeline.valid_from
            , {{ get_scd_valid_to('timeline.valid_to') }} as valid_to
        from timeline
        left join {{table1}}
            on timeline.{{join_id}} = {{table1}}.{{join_id}}
            and {{table1}}.valid_from <= timeline.valid_from 
            and {{table1}}.valid_to >= timeline.valid_to
        left join {{table2}}
            on timeline.{{join_id}} = {{table2}}.{{join_id}}
            and {{table2}}.valid_from <= timeline.valid_from 
            and {{table2}}.valid_to >= timeline.valid_to    
    )

select 
    *
from timeline_and_tables_joined
order by {{join_id}}, valid_from, valid_to

{% endmacro %}

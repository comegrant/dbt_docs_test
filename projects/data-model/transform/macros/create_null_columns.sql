{% macro generate_null_columns(count, prefix='col') %}
  {%- set cols = [] -%}
  {%- for i in range(1, count + 1) -%}
    {%- set col_name = prefix ~ '_' ~ i|string -%}
    {%- do cols.append("NULL AS " ~ col_name) -%}
  {%- endfor -%}
  {{ return(cols | join(', ')) }}
{% endmacro %}
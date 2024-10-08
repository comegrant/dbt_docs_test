{#
    Intermediate CTE that can be used to map financial product variation ids 
    to mealbox product variation ids.
#}

{%- set attributes_dict = {
    'preselected_mealbox_product_id': ['CB300C77-4D3E-4179-AC62-7AB391226E87']
} -%}

with 

attribute_values as (

    select * from {{ ref('product_layer__product_variation_attribute_values')}}
    
),


attribute_values_pivoted as (

    select
        product_variation_id
        , company_id

        {% for key, value in attributes_dict.items() -%}
            ,any_value(case
                when attribute_id in ({{ value | map('tojson') | join(', ') }})
                then attribute_value
            end, true) as {{ key }}

        {%- endfor %}

    from attribute_values
    group by all
),

attribute_values_pivoted_remove_nulls as (
    select
        product_variation_id
        , company_id
        , UPPER(preselected_mealbox_product_id) as preselected_mealbox_product_id
    from attribute_values_pivoted
    where preselected_mealbox_product_id is not null
)

select * from attribute_values_pivoted_remove_nulls
{%- set attributes_dict = {
    'preselected_mealbox_product_id': ['CB300C77-4D3E-4179-AC62-7AB391226E87']
} -%}

with 

attribute_values as (

    select * from {{ ref('product_layer__product_variation_attribute_values')}}
    
),


pivot_attribute_values as (

    select
        product_variation_id
        , company_id

        {% for key, value in attributes_dict.items() -%}
        {# use any value to XYZ #}
            ,any_value(case
                when attribute_id in ({{ value | map('tojson') | join(', ') }})
                then attribute_value
            end, true) as {{ key }}

        {%- endfor %}

    from attribute_values
    group by all
),

cast_pivot_attribute_values as (
    select
        product_variation_id
        , company_id
        , preselected_mealbox_product_id
    from pivot_attribute_values
)

select * from cast_pivot_attribute_values
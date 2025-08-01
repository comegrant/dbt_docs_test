with 

attribute_value_templates_filtered as (

    select * from {{ ref('int_product_variation_attribute_templates_filtered') }}

)

, attribute_value_templates_pivot as (

    select
    product_type_id
    , {{ dbt_utils.pivot(
                        'attribute_name'
                        , dbt_utils.get_column_values(
                            ref('int_product_variation_attribute_templates_filtered')
                            ,'attribute_name'
                        )
                        , agg='max'
                        , then_value='attribute_default_value'
                        , else_value='NULL'
                    )
    }}

    from attribute_value_templates_filtered
    group by product_type_id

)

, attribute_value_templates_clean as (
    select
        product_type_id
        , cast(meals as int) as meals
        , case
            when len(portions) = 2 and right(portions, 1) = '1'
            then cast(left(portions, 1) as int)
            else cast(portions as int)
        end as portions
        , case
            when len(portions) = 2 and right(portions, 1) = '1'
            then concat(left(Portions, 1), '+')
            else portions
        end as portion_name
        , cast(is_active as boolean) as is_active_product
        , cast(vat as int) as vat_rate
        , label_addition_standard as picking_line_label
        , cast(max_variation_qty_allowed as int) as maximum_order_quantity
    from attribute_value_templates_pivot
)

select * from attribute_value_templates_clean

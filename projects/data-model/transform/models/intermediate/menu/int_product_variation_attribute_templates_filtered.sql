with 

attribute_value_template as (

    select * from {{ ref('product_layer__product_variation_attribute_templates')}}

)

-- only include the attributes we want to fetch
, attribute_values_template_filter as (

    select
        attribute_id
        , attribute_name
        , attribute_default_value
        , product_type_id
    from attribute_value_template
    where attribute_name in
                        (
                            'meals'
                            ,'portions'
                            ,'is_active'
                            ,'vat'
                            ,'label_addition_standard'
                            ,'max_variation_qty_allowed'
                        )

)

select * from attribute_values_template_filter

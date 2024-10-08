with

source as (

    select * from {{ref('scd_cms__billing_agreement_basket_products_list')}}

)

, renamed as (

    select

        
        {# ids #}
        billing_agreement_basket_id

        {# objects #}
        , products_list as basket_products_list

        {# scd #}
        , dbt_valid_from as valid_from
        , {{ get_scd_valid_to('dbt_valid_to') }} as valid_to
        
        {# system #}
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
--THIS IS A TEMPORARY HARDCODED FIX
{{ config(
   post_hook= '''
    UPDATE {{ this }} 
    SET 
        basket_products_list = ARRAY(STRUCT("F2A85E75-D47B-4C5F-B7B2-A813BFACD68C" AS product_variation_id, 1 AS product_variation_quantity, false as is_extra))
    WHERE 
        basket_products_list = ARRAY(
            STRUCT("8105008F-93DC-4158-85C6-A9A548D7B4EF" AS product_variation_id, 1 AS product_variation_quantity, false as is_extra),
            STRUCT("F2A85E75-D47B-4C5F-B7B2-A813BFACD68C" AS product_variation_id, 1 AS product_variation_quantity, false as is_extra)
        )
   '''
) }}

with

source as (

    select * from {{ref('scd_cms__billing_agreement_basket_products_list')}}

)

, history as (

    select *
    from {{ ref('analyticsdb_analytics__billing_agreement_basket_products_list_history') }}

)

, source_renamed as (

    select
        
        {# ids #}
        billing_agreement_basket_id

        {# objects #}
        , products_list as basket_products_list

        {# scd #}
        , convert_timezone('Europe/Oslo', 'UTC', dbt_valid_from) as valid_from
        , coalesce(convert_timezone('Europe/Oslo', 'UTC', dbt_valid_to), cast('{{ var("future_proof_date") }}' as timestamp)) as valid_to
        
        {# system #}
        , convert_timezone('Europe/Oslo', 'UTC', updated_at) as source_updated_at
        , updated_by as source_updated_by

    from source

)

, source_min_valid_from (
    select
        billing_agreement_basket_id
        , min(valid_from) as min_valid_from
    from source_renamed
    group by 1
)

, history_before_source (

    select 
        history.billing_agreement_basket_id
        , history.basket_products_list
        , history.valid_from
        , case 
            -- Use source first valid to in current row from the history
            when history.valid_to = '{{ var("future_proof_date") }}'
            then source_min_valid_from.min_valid_from
            -- Use source first valid to when history is overlapping
            when history.valid_to > source_min_valid_from.min_valid_from
            then source_min_valid_from.min_valid_from
            else history.valid_to
        end as valid_to
        , history.source_created_at as source_updated_at
        , history.source_created_by as source_updated_by
    from history
    left join source_min_valid_from
        on history.billing_agreement_basket_id = source_min_valid_from.billing_agreement_basket_id
    -- Only include history from before the source snapshot
    where history.valid_from < source_min_valid_from.min_valid_from

)

, source_history_unioned ( 

    select * from history_before_source

    union 

    select * from source_renamed

)

select * from source_history_unioned

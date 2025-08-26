with

deviations as (

    select * from {{ ref('int_basket_deviation_products_joined') }}

)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, deviations_onesub_migration as (

    select
        deviations.billing_agreement_basket_deviation_id
        , deviations.billing_agreement_id
        , deviations.billing_agreement_basket_id
        , deviations.company_id
        , products.product_id
        , products.meals
        , products.portions
        , deviations.product_variation_id
        , deviations.product_variation_quantity
        , case
            -- Customers that had made deviations prior to the Onesub migration was 
            -- migrated by overwriting their latest deviation product
            -- hence the Onesub product was valid from the updated timestamp of the deviation product
            when deviations.is_onesub_migration_update = 1
                then deviations.deviation_product_updated_at
            -- Customers that had not made any deviations prior to the Onesub migration
            -- was migrated by creating a deviation using the mealselector as origin
            -- hence the Onesub product was valid from the creation timestamp of the deviation
            else deviations.deviation_created_at
        end as valid_from
        , 'onesub migration deviation' as basket_source
    from deviations
    inner join products
        on deviations.product_variation_id = products.product_variation_id
        and deviations.company_id = products.company_id
        and products.product_id = '{{ var("onesub_product_id") }}'
    where deviations.is_onesub_migration_update = 1
    or deviations.is_onesub_migration_insert = 1

)

select * from deviations_onesub_migration

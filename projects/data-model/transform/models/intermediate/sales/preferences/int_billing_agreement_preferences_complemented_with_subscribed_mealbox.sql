with

preference_list as (

    select * from {{ref('cms__billing_agreements_preferences_list')}}

)

, preferences as (

    select * from {{ref('cms__preferences')}}

)

, basket_product_variations_list as (

    select * from {{ref('cms__billing_agreement_basket_products_list')}}    

)

, baskets as (

    select * from {{ref('cms__billing_agreement_baskets')}}
    where valid_to = '{{ var("future_proof_date") }}'

)

, billing_agreements as (

    select * from {{ref('cms__billing_agreements')}}
    where valid_to = '{{ var("future_proof_date") }}'

)

, preference_attributes as (

    select * from {{ref('cms__preference_attribute_values')}}

)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

------------------------------------------------------------------------------------------------------
-- In the following we get the concept preference as an array from the subscribed mealbox product,  --
-- as not all concept preferences are in billing_agreement_preferences                              --
------------------------------------------------------------------------------------------------------

, basket_product_variations_exploded as (
    select 
    billing_agreement_basket_id
    , basket_products_item.product_variation_id
    , valid_from
    , valid_to
    from basket_product_variations_list
    lateral view explode(basket_products_list) as basket_products_item
)

, subscribed_mealbox_products as (
    select 
    baskets.billing_agreement_id
    , billing_agreements.company_id
    , billing_agreements.signup_date
    , products.product_id
    , basket_product_variations_exploded.valid_from
    , basket_product_variations_exploded.valid_to
    from 
    basket_product_variations_exploded
    left join baskets
    on basket_product_variations_exploded.billing_agreement_basket_id = baskets.billing_agreement_basket_id 
    left join billing_agreements
    on billing_agreements.billing_agreement_id = baskets.billing_agreement_id
    left join products
    on products.product_variation_id = basket_product_variations_exploded.product_variation_id
    and billing_agreements.company_id = products.company_id
    where products.product_type_id = '2F163D69-8AC1-6E0C-8793-FF0000804EB3' --Mealboxes
    
)

, preferences_and_products_mapped as (

    select 
    preference_id
    , attribute_value as product_id
    from preference_attributes
    where attribute_id = '680D1B8A-F967-4713-A270-8E30D683F4B8' --linked_mealbox_id
)

, subscribed_mealbox_add_preference_id as (

    select 
    
    subscribed_mealbox_products .billing_agreement_id
    , subscribed_mealbox_products .company_id
    , preferences_and_products_mapped.preference_id
    , case
        when subscribed_mealbox_products.product_id = 'D699150E-D2DE-4BC1-A75C-8B70C9B28AE3' --Onesub product
        then true
        else false
    end is_onesub
    , valid_from
    , valid_to
    
    from subscribed_mealbox_products  
    left join preferences_and_products_mapped
    on preferences_and_products_mapped.product_id = subscribed_mealbox_products.product_id
    
    where preferences_and_products_mapped.preference_id is not null 
    or subscribed_mealbox_products.product_id = 'D699150E-D2DE-4BC1-A75C-8B70C9B28AE3' --Onesub product
    
)

, subscribed_mealbox_preference_list_missing_valid_to as (

    select 
    
        billing_agreement_id
        , valid_from
        , max(company_id) as company_id
        , max(is_onesub) as is_onesub
        , array_sort(collect_list(preference_id)) as concept_preference_id_list
    
    from subscribed_mealbox_add_preference_id 
    group by 1, 2
)   

, subscribed_mealbox_preference_list as (

    select 
    
        billing_agreement_id
        , valid_from
        , {{ get_scd_valid_to('valid_from', 'billing_agreement_id') }} as valid_to
        , company_id
        , is_onesub
        , concept_preference_id_list
    
    from subscribed_mealbox_preference_list_missing_valid_to
)   

--------------------------------------------------------------------------------
-- In the following we find all rows in the preference list that has concepts --
-- this will be used to unify the ones with and without concepts              --
--------------------------------------------------------------------------------

, preferences_exploded as (
    select 
    billing_agreement_id
    , preference_item
    , valid_from
    ,valid_to
    from preference_list
    lateral view explode(preference_id_list) as preference_item
)

, concept_preferences as (

    select 
    distinct 
    billing_agreement_id
    , valid_from
    , valid_to
    from preferences_exploded
    left join preferences
    on preferences.preference_id = preferences_exploded.preference_item
    where preference_type_id= '009CF63E-6E84-446C-9CE4-AFDBB6BB9687' -- Preference type name is "Concept"
        
)

, taste_preferences_only as (

    select 
    preference_list.*
    from 
    preference_list
    left join concept_preferences
    on preference_list.billing_agreement_id = concept_preferences.billing_agreement_id
    and preference_list.valid_from = concept_preferences.valid_from
    where concept_preferences.billing_agreement_id is null

)

--Combining the subscribed mealboxes with the taste preferences in a unified timeline
--This is needed since the mealboxes are coming from the basket product scd table, while the taste preferences are coming from the agreement preference table
, taste_preferences_and_subscribed_mealbox_joined as (

    {{join_snapshots('subscribed_mealbox_preference_list', 'taste_preferences_only', 'billing_agreement_id', 'preference_id_list')}}

)

--Putting the concept preferences and taste preferences in the same array
, combined_list as (
    select 
    billing_agreement_id
    , company_id
    , is_onesub
    , array_sort(
        array_union(
            coalesce( preference_id_list, array() ), 
            coalesce( concept_preference_id_list, array() )
        )
    ) as preference_id_list_one_list
    , valid_from
    , valid_to
    from taste_preferences_and_subscribed_mealbox_joined
)

--Unifying the preferences from the preference list with the preferences found by the mealbox
, billing_agreement_preferences_unioned as (

    {{join_snapshots('combined_list', 'preference_list', 'billing_agreement_id', 'preference_id_list')}}

)

--Combining everything
, joined as (

    select 
        billing_agreement_preferences_unioned.billing_agreement_id
        , billing_agreements.company_id
        , array_union(coalesce(preference_id_list,array()),coalesce(preference_id_list_one_list,array())) as preference_id_list
        , billing_agreement_preferences_unioned.valid_from
        , billing_agreement_preferences_unioned.valid_to
        , 'basket_products and billing_agreement_preferences' as source
    from billing_agreement_preferences_unioned
    left join billing_agreements
    on billing_agreement_preferences_unioned.billing_agreement_id = billing_agreements.billing_agreement_id

)

select * from joined
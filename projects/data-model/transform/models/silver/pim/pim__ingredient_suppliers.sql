with

source as (

    select * from {{ source('pim', 'pim__suppliers') }}

)

, renamed as (

    select

        {# ids #}
        supplier_id as ingredient_supplier_id
        , status_code_id as ingredient_supplier_status_code_id
        , country_id

        {# system #}
        , created_by as source_created_by
        , modified_by as source_updated_by
        , created_date as source_created_at
        , modified_date as source_updated_at

        {# strings #}
        , supplier_name as ingredient_supplier_name

    {# other columns that are not added yet
    , timeblock_id
    , inventory_ledger_id
    , serial_number
    , supplier_type_id
    #}

    from source

)

select * from renamed
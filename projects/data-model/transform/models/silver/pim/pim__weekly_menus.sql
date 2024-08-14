with 

source as (

    select * from {{ source('pim', 'pim__weekly_menus') }}

),

renamed as (

    select
        
        {# ids #}
        weekly_menus_id as weekly_menu_id
        , company_id
        , status_code_id as weekly_menu_status_code_id

        {# numerics #}
        , menu_week
        , menu_year

        {# dates #}
        , {{ get_iso_week_start_date('menu_year', 'menu_week') }} as delivery_week_monday_date

        {# not sure if these are needed:
        , ordering_date
        , default_delivery_date
        #}

    from source

)

select * from renamed
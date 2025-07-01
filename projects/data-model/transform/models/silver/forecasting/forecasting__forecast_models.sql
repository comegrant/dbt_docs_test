with 

source as (

    select * from {{ source('forecasting', 'forecast_models') }}

)

, renamed as (

    select 
        {# ids #}
        forecast_model_id

        {# strings #}
        , forecast_model_name

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed

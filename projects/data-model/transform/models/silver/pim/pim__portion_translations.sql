with 

source as (

    select * from {{ source('pim', 'pim__portions_translations') }}

),

renamed as (

    select
        
        {# ids #}
        portion_id
        , language_id

        {# strings #}
        , 
        -- Rename plus portions to be 2+, 3+, etc. instead of 21, 31, etc.
        case
            when len(portion_name) = 2 and right(portion_name, 1) = '1'
            then concat(left(portion_name, 1), '+')
            else portion_name
        end as portion_name
        
        {# not sure if these are needed
        , portion_description
        #}

    from source

)

select * from renamed
with

source as (
    select * from {{ref('seed__customer_journey_segments_mapping')}}
),

renamed as (
    select

        {# ids #}
        cast(main_segment_id as int) as customer_journey_main_segment_id
        , cast(sub_segment_id as int) as customer_journey_sub_segment_id

        {# strings #}
        , main_segment_name as customer_journey_main_segment_name
        , sub_segment_name as customer_journey_sub_segment_name
        , main_segment_description as customer_journey_main_segment_description
        , sub_segment_description as customer_journey_sub_segment_description

    from source
)

select * from renamed

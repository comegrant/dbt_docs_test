with

customer_journey_segments as (

    select * from {{ref('data_platform__customer_journey_segments')}}

)

, add_pk as (
    select
        md5( cast( customer_journey_sub_segment_id as string) ) as pk_dim_customer_journey_segments
        , customer_journey_main_segment_id
        , customer_journey_sub_segment_id
        , customer_journey_main_segment_name
        , customer_journey_sub_segment_name
        , customer_journey_main_segment_description
        , customer_journey_sub_segment_description

    from customer_journey_segments
)

select * from add_pk

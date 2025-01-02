with preselector_failed_output as (

    select * from {{ source('mloutputs', 'preselector_failed_realtime_output') }}

)

, parsed as (

    select
        error_message,
        error_code,
        from_json(request, 'struct<
            agreement_id: bigint,
            company_id: string,
            compute_for: array<struct<week: int, year: int>>,
            concept_preference_ids: array<string>,
            taste_preferences: array<string>,
            portion_size: int,
            number_of_recipes: int,
            override_deviation: boolean,
            ordered_weeks_ago: map<string,int>,
            has_data_processing_consent: boolean,
            correlation_id: string
        >') as parsed_request
    from preselector_failed_output

)

, renamed as (

select
    parsed_request.correlation_id,
    parsed_request.agreement_id as billing_agreement_id,
    parsed_request.company_id,
    parsed_request.compute_for as requested_menu_year_weeks,
    parsed_request.concept_preference_ids,
    parsed_request.taste_preferences as taste_preference_ids,
    parsed_request.portion_size as requested_portions,
    parsed_request.number_of_recipes as requested_meals,
    parsed_request.override_deviation as is_override_deviation,
    parsed_request.ordered_weeks_ago as requested_main_recipe_quarantining_control,
    parsed_request.has_data_processing_consent,
    error_message,
    error_code
from parsed

)

select * from renamed

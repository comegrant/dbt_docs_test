with

portions as (
    select * from {{ ref('pim__portions') }}
)

, portion_translations as (
    select * from {{ ref('pim__portion_translations') }}
)

, status_names as (
    select * from {{ ref('pim__status_code_translations') }}
)

, languages as (

    select distinct language_id from {{ ref('cms__countries')}}

)

, joined as (
    select
        md5(concat(portions.portion_id, portion_translations.language_id)) as pk_dim_portions
        ,portions.portion_id
        ,portion_translations.language_id as language_id
        ,portions.portion_size as portions
        ,portion_translations.portion_name as portion_name_local
        ,portion_translations_english.portion_name as portion_name_english
        ,portions.portion_status_code_id
        ,status_names.status_code_name as portion_status_name_local
        ,status_names_english.status_code_name as portion_status_name_english
    from
        portions
        left join portion_translations
            on portions.portion_id = portion_translations.portion_id
        left join status_names
            on portions.portion_status_code_id = status_names.status_code_id
            and portion_translations.language_id = status_names.language_id
        left join status_names as status_names_english
            on portions.portion_status_code_id = status_names_english.status_code_id
            and status_names_english.language_id = 4 -- English
        left join portion_translations as portion_translations_english
            on portions.portion_id = portion_translations_english.portion_id
            and portion_translations_english.language_id = 4 -- English
        left join languages
            on portion_translations.language_id = languages.language_id
        where languages.language_id is not null
)

, add_unrelevant_row as (

    select
        '0' as pk_dim_portions
        , 0 as portion_id
        , 0 as language_id
        , 0 as portions
        , 'Not relevant' as portion_name_local
        , 'Not relevant' as portion_name_english
        , '0' as portion_status_code_id
        , 'Not relevant' as portion_status_name_local
        , 'Not relevant' as portion_status_name_english

    union all

    select * from joined
)

select * from add_unrelevant_row

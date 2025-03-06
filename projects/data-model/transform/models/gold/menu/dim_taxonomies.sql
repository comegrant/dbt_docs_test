with

taxonomies as (
    select * from {{ ref('pim__taxonomies') }}
),

taxonomy_translations as (
    select * from {{ ref('pim__taxonomy_translations') }}
),

taxonomy_types as (
    select * from {{ ref('pim__taxonomy_types') }}
),

statuses as (
    select * from {{ ref('pim__status_code_translations') }}
),

local_languages as (
    select distinct language_id from {{ ref('cms__countries') }}
),

all_tables_joined as (
    select
        -- There are taxonomies that have translations in multiple local languages, so the PK needs to account for this
        md5(cast(concat(taxonomies.taxonomy_id, taxonomy_translations.language_id) as string)) as pk_dim_taxonomies
        , taxonomies.taxonomy_id
        , taxonomy_translations.language_id
        , taxonomy_translations.taxonomy_name as taxonomy_name_local
        , taxonomy_translations_english.taxonomy_name as taxonomy_name_english
        , statuses.status_code_name as taxonomy_status_name_local
        , statuses_english.status_code_name as taxonomy_status_name_english
        , taxonomy_types.taxonomy_type_name as taxonomy_type_name

    from taxonomies
    left join taxonomy_translations
        on taxonomy_translations.taxonomy_id = taxonomies.taxonomy_id
    left join taxonomy_translations as taxonomy_translations_english
        on taxonomies.taxonomy_id = taxonomy_translations_english.taxonomy_id
        and taxonomy_translations_english.language_id = {{ var('english_language_id') }}
    left join taxonomy_types
        on taxonomy_types.taxonomy_type_id = taxonomies.taxonomy_type_id
    left join statuses
        on statuses.status_code_id = taxonomies.taxonomy_status_code_id
        and statuses.language_id = taxonomy_translations.language_id
    left join statuses as statuses_english
        on statuses_english.status_code_id = taxonomies.taxonomy_status_code_id
        and statuses_english.language_id = {{ var('english_language_id') }}
    left join local_languages
        on taxonomy_translations.language_id = local_languages.language_id
    -- This is to ensure that we only have one row per taxonomy and local language with English translations as columns
    -- ASSUMPTION: We don't deliver to a country which has English as a local language
    where local_languages.language_id is not null
)

select * from all_tables_joined

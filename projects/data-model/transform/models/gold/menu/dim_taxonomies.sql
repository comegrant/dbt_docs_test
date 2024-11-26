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


all_tables_joined as (
    select
        md5(cast(concat(taxonomies.taxonomy_id, taxonomy_translations.language_id) as string)) as pk_dim_taxonomies
        , taxonomies.taxonomy_id
        , taxonomy_translations.language_id
        , taxonomy_translations.taxonomy_name
        , statuses.status_code_name as taxonomy_status_code_name
        , taxonomy_types.taxonomy_type_name

    from taxonomies
    left join taxonomy_translations on taxonomy_translations.taxonomy_id = taxonomies.taxonomy_id
    left join taxonomy_types on taxonomy_types.taxonomy_type_id = taxonomies.taxonomy_type_id
    left join statuses on statuses.status_code_id = taxonomies.taxonomy_status_code_id and statuses.language_id = taxonomy_translations.language_id

)

select * from all_tables_joined

select
    taxonomy_type_name
    , taxonomy_id
    , language_id
    , taxonomy_name_local
    , taxonomy_name_english
from gold.dim_taxonomies
where taxonomy_type_name like 'seo_%'
and language_id = {language_id}
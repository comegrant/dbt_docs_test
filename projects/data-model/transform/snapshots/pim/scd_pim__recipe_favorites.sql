{% snapshot scd_pim__recipe_favorites %}

select * from {{ source('pim', 'pim__recipe_favorites') }}

{% endsnapshot %}

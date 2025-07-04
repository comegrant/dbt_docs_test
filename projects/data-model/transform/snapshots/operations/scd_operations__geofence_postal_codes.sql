{% snapshot scd_operations__geofence_postal_codes %}

select * from {{ source('operations', 'operations__geofence_postalcodes') }}

{% endsnapshot %} 
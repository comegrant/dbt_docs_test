{% snapshot scd_operations__geofences %}

select * from {{ source('operations', 'operations__geofence') }}

{% endsnapshot %} 
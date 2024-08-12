{%- set addon_subscriptions_dict = {
    'has_raatass_kickback': [
        'A91FEBD9-A92A-488F-9A8B-AEE55DD221C6', 
        '4A2B0274-3D9E-4D70-9D34-CB2BA452BF88'
        ],
    'has_digital_recipes': [
        'F30B7DF2-595E-4B0B-989C-3EA91437B480',
        'FA2BC3E5-C1B3-47D8-9B5A-09F19AE19192',
        '05464097-B909-4A19-992D-7B4E7ADDD00B',
        '5E797AB8-F9EE-46CA-9EA0-0B205F73CE8A'
        ],
    'is_test_account': [
        '4E286DB4-36E1-4CDF-A7F5-2A56A8C73A51',
        'D20DE463-6EE9-406F-823D-3A5F565D9049',
        '3B6EECDB-8FF6-44D4-B1D3-5B6136E9993E',
        '00BE3C2C-FDE7-4F98-B4B2-80D8BC2B16E2'
        ],
    'is_business_account': ['ED31C587-D0F9-4650-82B3-D9FCFE476DF0'],

} -%}

with 

addon_subscriptions as (

    select * from {{ ref('sil_cms__billing_agreement_addon_subscriptions')}}
    
),

addon_names as (

    select * from {{ ref('sil_cms__addon_subscriptions')}}
    
),

pivot_and_join_addon_subscriptions_and_addon_names as (

    select
        addon_subscriptions.agreement_id

        {% for key, value in addon_subscriptions_dict.items() -%}

            ,case
                when addon_names.addon_subscription_id in ({{ value | map('tojson') | join(', ') }}) and 
                    addon_subscriptions.is_active = true
                then true
                else false
            end as {{ key }}

        {%- endfor %}

    from addon_subscriptions
    left join addon_names
    on addon_subscriptions.addon_subscription_id = addon_names.addon_subscription_id
)

select * from pivot_and_join_addon_subscriptions_and_addon_names
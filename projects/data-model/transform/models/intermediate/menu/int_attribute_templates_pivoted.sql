{%- set attributes_dict = {
    'number_of_meals': [
        'EDF04536-BC72-41FC-9491-024DD3E48FFF',
        '04974035-6A0A-4BC0-87C6-11138BB5B08F',
        '462A097B-2B57-4CD0-AF38-26BAC99B6020',
        '4FF0F041-CEEB-4DBB-B191-7C2F3BFEF225',
        'E69BB4A1-94A4-4AE3-B527-89A51B70D613',
        'DBE23C41-F6AF-490B-9ED1-B57B74340500',
        '1C133AED-A7D9-4B88-8EBA-B7634448BAEA',
        '9CCE27FA-DDB9-4EC7-82FE-C82F9C987F0A',
        '2F3B3E18-82D3-48A8-9DC6-DCDA96AA4EEC',
        '4D4F9B2C-A876-4A1F-B17C-F4500F577202',
        '9221B4A2-2647-42D7-9E25-F97D0023A2B7',
        '8BA21161-51AC-4794-9FFA-F9EB2BDC11E0'
        ],
    'number_of_portions': [
        'B438F8A6-3E2A-4C40-A930-0093FE84EC75',
        '55BDD7B1-76B6-4AC5-9F40-0D7A08CF9BC1',
        'B2D311ED-EE33-4CE7-89A1-10330A82E7E8',
        '0AB58A86-6B14-429C-B9A0-4A4D17132C0F',
        '9CABEACD-B730-40DD-81FE-6092B7FAB066',
        'B1425946-B5A7-4D81-A5F7-6F92863009BD',
        'C4205C73-44B5-4F88-BB85-8F56C2CB92B1',
        'E310C663-8226-4B16-8A7A-91C12309FEE6',
        'F52FC09F-AC4C-46B6-B9CE-9881BCC49743',
        'F6FAF781-BF7A-48DE-94D1-A26C4505BC88',
        '8DB1BF15-9D1B-492F-B69E-C7A37F95FEEF',
        'D376B880-C630-434C-9EB3-E4009DBBCF8C',
        'B1BEE06A-36AE-418F-B493-EEAB35B7BE3E'
        ]
} -%}

with 

attribute_templates as (

    select * from {{ ref('sil_product_layer__product_variation_attribute_templates')}}
    
),

pivot_attribute_templates as (

    select
        product_type_id

        {% for key, value in attributes_dict.items() -%}
        {# use any value to XYZ #}
            ,any_value(case
                when attribute_id in ({{ value | map('tojson') | join(', ') }})
                then attribute_default_value
            end, true) as {{ key }}

        {%- endfor %}

    from attribute_templates
    group by all
),

cast_pivot_attribute_templates as (
    select
        product_type_id
        , cast(number_of_meals as int) as number_of_meals
        , cast(number_of_portions as int) as number_of_portions
    from pivot_attribute_templates
)

select * from cast_pivot_attribute_templates
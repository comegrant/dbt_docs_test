with 

source as (

    select * from {{ source('cms', 'cms_billing_agreement_order') }}

),

renamed as (

    select
        {# ids #}
        id as cms_order_id
        , order_id as ops_order_id
        , order_type as order_type_id
        , order_status_id
        , agreement_id
        {# not yet included columns
        --shipping_address_id
        --billing_address_id
        --timeblock as timeblock_id
        --original_timeblock as original_timeblock_id
        --payment_partner_id
        --order_campaign_id
        --transaction_id
        --invoice_processed as invoice_processed_id
        #}

        {# strings #}
        {# not yet included columns
        --payment_method
        --invoice_status
        --transactionnumber as transaction_number
        #}

        {# numerics #}
        , year as delivery_year
        , week as delivery_week
        {# not yet included columns
        --totalamount as total_amount
        --sumoforderlinesprice as sum_of_order_lines_price
        --version_shippingaddress as version_billing_address
        --version_billingaddress as version_shipping_address
        #}

        {# booleans #}
        , has_recipe_leaflets
        {# not yet included columns
        --senttoinvoicepartner as is_sent_to_invoice_partner
        #}

        {# dates #}
        , {{ get_iso_week_start_date('year', 'week') }} as delivery_week_monday_date
        {# not yet included columns
        --cutoff_date
        --delivery_date
        --invoice_status_date.
        #}

        {# timestamps #}
        , created_date as order_created_at
        {# not yet included columns
        --transaction_date as transaction_at
        --invoice_data_create as invoice_data_created_at
        --invoice_processed_at
        #}

    from source

)

select * from renamed
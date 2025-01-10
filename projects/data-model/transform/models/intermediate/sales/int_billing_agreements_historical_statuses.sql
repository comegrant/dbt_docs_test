{{ config(materialized='table', tags=['static']) }}

with 

statuses_except_deleted as (
    select * from {{ ref('analyticsdb_snapshots__historical_billing_agreement_statuses')}}
)

, latest_snapshot_date as (
    select 
        date(max(source_created_at)) as latest_snapshot_date
    from statuses_except_deleted
)

-- If an agreement is deleted, it is not included in the statuses_except_deleted.
-- Therefore we need to add a row for each deleted agreement with the status 40 (deleted) 
-- and valid_from being the day after the last logged non-deleted status: max(source_created_at) + interval 1 day
, deleted_agreements as (
    select 
        billing_agreement_id
        , 40 as billing_agreement_status_id -- 40 is the status id for deleted agreements
        , max(source_created_at) + interval 1 day as valid_from
    from statuses_except_deleted
    group by 1
    having max(source_created_at) < (select latest_snapshot_date from latest_snapshot_date)
)

, statuses_included_deleted as (

    select 
    billing_agreement_id
    , billing_agreement_status_id
    , source_created_at as valid_from
    from statuses_except_deleted
    
    union all
    
    select 
    billing_agreement_id
    , billing_agreement_status_id
    , valid_from
     from deleted_agreements

)

, statuses_add_valid_to as (
    select 
        billing_agreement_id
        , billing_agreement_status_id
        , valid_from
        , {{get_scd_valid_to('valid_from', 'billing_agreement_id')}} as valid_to
    from statuses_included_deleted
)

-- group consecutive rows with same status
, statuses_grouped as (
    select 
        billing_agreement_id
        , billing_agreement_status_id
        , valid_from
        , valid_to
        , row_number() over 
            (
                partition by 
                    billing_agreement_id 
                order by valid_from
            ) 
            - 
            row_number() over 
            (
                partition by 
                    billing_agreement_id
                    , billing_agreement_status_id
                order by valid_from
            ) 
        as group
    from 
        statuses_add_valid_to
)

-- merge consecutive rows with same status
, statuses_merge_groups as (
    select
        billing_agreement_id
        , billing_agreement_status_id
        , group
        , min(valid_from) as valid_from
        , max(valid_to) as valid_to
    from 
        statuses_grouped
    group by
        all
)

select * from statuses_merge_groups


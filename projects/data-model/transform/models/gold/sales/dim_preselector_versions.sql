with

preselector_successful_output as (

    select * from {{ ref('mloutputs__preselector_successful_realtime_output') }}
)

, grouped_by_model_version as (

    select
        model_version_commit_sha
    , min(created_at) as source_model_version_first_used_at
    , max(created_at) as source_model_version_latest_used_at
    from preselector_successful_output
    group by model_version_commit_sha
)

, split_sha as (

    select
        model_version_commit_sha as commit_sha
    , left(model_version_commit_sha, 7) as short_commit_sha
    , source_model_version_first_used_at
    , source_model_version_latest_used_at
    from grouped_by_model_version

)

, add_version_number as (

    select
        commit_sha
    , short_commit_sha
    , source_model_version_first_used_at
    , source_model_version_latest_used_at
    , row_number() over (order by source_model_version_first_used_at asc) as version_number
    from split_sha

)

, add_pk as (

    select
        md5(commit_sha) as pk_dim_preselector_versions
    , commit_sha
    , short_commit_sha
    , source_model_version_first_used_at
    , source_model_version_latest_used_at
    , version_number
    from add_version_number

)

select * from add_pk

{{ config(
    materialized='table'
) }}

with src as (

    select
        job_name      as job_id,
        inst_id       as instance_id,
        "user"        as user_id,
        status        as job_status,
        start_time,
        end_time
    from {{ ref('bronze_job_events') }}

)

select
    job_id,
    instance_id,
    user_id,
    job_status,
    start_time,
    end_time,
    case
        when end_time is not null then end_time - start_time
        else null
    end as run_time_sec
from src


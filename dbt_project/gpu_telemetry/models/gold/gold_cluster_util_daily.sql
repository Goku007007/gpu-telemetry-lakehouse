{{ config(
    materialized='table'
) }}

with base as (

    select
        -- Convert integer ts (seconds) to a timestamp, then truncate to day
        date_trunc('day', to_timestamp(ts)) as dt,
        gpu_util_pct,
        cpu_util_pct
    from {{ ref('silver_gpu_timeseries') }}
    where gpu_util_pct is not null

),

agg as (

    select
        dt,
        avg(gpu_util_pct) as avg_gpu_util,
        quantile_cont(gpu_util_pct, 0.95) as p95_gpu_util,
        avg(cpu_util_pct) as avg_cpu_util
    from base
    group by dt

)

select *
from agg
order by dt


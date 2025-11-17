{{ config(
    materialized='table'
) }}

with src as (

    select
        worker_name,
        machine            as machine_id,
        start_time,
        end_time,
        machine_cpu_iowait,
        machine_cpu_kernel,
        machine_cpu_usr,
        machine_gpu,
        machine_load_1,
        machine_net_receive,
        machine_num_worker,
        machine_cpu
    from {{ ref('bronze_machine_metrics') }}

)

select
    machine_id,
    worker_name,
    end_time                          as ts,  -- treat end of the window as the timestamp
    machine_gpu                       as gpu_util_pct,
    machine_cpu                       as cpu_util_pct,
    machine_load_1,
    machine_net_receive,
    machine_cpu_iowait,
    machine_cpu_kernel,
    machine_cpu_usr,
    machine_num_worker
from src
where end_time is not null


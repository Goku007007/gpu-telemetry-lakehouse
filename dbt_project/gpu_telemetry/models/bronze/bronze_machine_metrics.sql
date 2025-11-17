{{ config(
    materialized='view'
) }}

select *
from read_parquet('../../data_lake/bronze/bronze_machine_metrics.parquet')



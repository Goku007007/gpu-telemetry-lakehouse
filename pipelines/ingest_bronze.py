# pipelines/ingest_bronze.py
import pandas as pd
from pathlib import Path

RAW_DIR = Path("data_raw")
BRONZE_DIR = Path("data_lake/bronze")
BRONZE_DIR.mkdir(parents=True, exist_ok=True)


def ingest_jobs():
    """Alibaba job table -> bronze_job_events.parquet"""
    src = RAW_DIR / "pai_job_table.csv"
    print(f"Reading {src} ...")
    df = pd.read_csv(src)
    out = BRONZE_DIR / "bronze_job_events.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {len(df):,} rows to {out}")


def ingest_instances():
    """Alibaba instance table (per-task info) -> bronze_instance_table.parquet"""
    src = RAW_DIR / "pai_instance_table.csv"
    print(f"Reading {src} ...")
    df = pd.read_csv(src)
    out = BRONZE_DIR / "bronze_instance_table.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {len(df):,} rows to {out}")


def ingest_machine_metrics():
    """Machine metrics -> bronze_machine_metrics.parquet"""
    src = RAW_DIR / "pai_machine_metric.csv"
    print(f"Reading {src} ...")
    df = pd.read_csv(src)
    out = BRONZE_DIR / "bronze_machine_metrics.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {len(df):,} rows to {out}")


def ingest_machine_specs():
    """Machine specs -> bronze_machine_spec.parquet"""
    src = RAW_DIR / "pai_machine_spec.csv"
    print(f"Reading {src} ...")
    df = pd.read_csv(src)
    out = BRONZE_DIR / "bronze_machine_spec.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {len(df):,} rows to {out}")


def ingest_gpu_specs():
    """
    GPU specs -> bronze_gpu_specs.parquet

    Using tpu_gpus.csv as our GPU spec source.
    """
    src = RAW_DIR / "tpu_gpus.csv"
    print(f"Reading {src} ...")
    df = pd.read_csv(src)
    out = BRONZE_DIR / "bronze_gpu_specs.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {len(df):,} rows to {out}")


def main():
    ingest_jobs()
    ingest_instances()
    ingest_machine_metrics()
    ingest_machine_specs()
    ingest_gpu_specs()


if __name__ == "__main__":
    main()

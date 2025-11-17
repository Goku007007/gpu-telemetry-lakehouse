from pathlib import Path

import duckdb
import pandas as pd
import matplotlib.pyplot as plt

DB_PATH = Path("telemetry.db")
OUT_DIR = Path("docs/images")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def main():
    con = duckdb.connect(DB_PATH)
    df = con.execute(
        """
        select
            dt::timestamp as dt,
            avg_gpu_util,
            anomaly_flag
        from gold_cluster_util_daily_scored
        order by dt
        """
    ).df()
    con.close()

    print(f"Loaded {len(df)} rows for plotting")

    fig, ax = plt.subplots(figsize=(10, 4))

    # Plot all days
    ax.plot(df["dt"], df["avg_gpu_util"], marker="o", linestyle="-", label="avg_gpu_util")

    # Highlight anomalies
    anomalies = df[df["anomaly_flag"] == 1]
    ax.scatter(anomalies["dt"], anomalies["avg_gpu_util"], marker="o", s=80, label="anomaly")

    ax.set_title("Daily Cluster GPU Utilization (with Anomalies)")
    ax.set_xlabel("Date")
    ax.set_ylabel("avg_gpu_util (dataset units)")
    ax.legend()
    fig.autofmt_xdate()

    out_path = OUT_DIR / "daily_gpu_util_with_anomalies.png"
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    print(f"Saved plot to {out_path}")


if __name__ == "__main__":
    main()


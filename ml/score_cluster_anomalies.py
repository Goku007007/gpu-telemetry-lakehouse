# ml/score_cluster_anomalies.py
from pathlib import Path

import duckdb
import pandas as pd
from joblib import load
from sklearn.preprocessing import StandardScaler

DB_PATH = Path("telemetry.db")
MODEL_PATH = Path("ml/cluster_anomaly_iforest.joblib")
SCALER_PATH = Path("ml/cluster_anomaly_scaler.joblib")


def load_data() -> pd.DataFrame:
    con = duckdb.connect(DB_PATH)
    df = con.execute(
        """
        select
            dt,
            avg_gpu_util,
            p95_gpu_util,
            avg_cpu_util
        from gold_cluster_util_daily
        order by dt
        """
    ).df()
    con.close()
    return df


def main():
    df = load_data()
    print(f"Scoring {len(df)} daily rows...")

    feature_cols = ["avg_gpu_util", "p95_gpu_util", "avg_cpu_util"]
    X = df[feature_cols].values

    model = load(MODEL_PATH)
    scaler: StandardScaler = load(SCALER_PATH)

    X_scaled = scaler.transform(X)

    # IsolationForest: predict returns 1 for normal, -1 for anomaly
    preds = model.predict(X_scaled)
    scores = model.decision_function(X_scaled)

    df["anomaly_flag"] = (preds == -1).astype(int)
    df["anomaly_score"] = scores

    # Write back into DuckDB as a new table
    con = duckdb.connect(DB_PATH)
    con.execute("drop table if exists gold_cluster_util_daily_scored")
    con.execute(
        """
        create table gold_cluster_util_daily_scored as
        select * from df
        """
    )
    con.close()

    print("Wrote gold_cluster_util_daily_scored to DuckDB")


if __name__ == "__main__":
    main()


# ml/train_cluster_anomaly_model.py
from pathlib import Path

import duckdb
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from joblib import dump

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
        """
    ).df()
    con.close()
    return df


def main():
    df = load_data()
    print(f"Loaded {len(df)} daily rows from gold_cluster_util_daily")

    feature_cols = ["avg_gpu_util", "p95_gpu_util", "avg_cpu_util"]
    X = df[feature_cols].values

    # Scale features for IsolationForest
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = IsolationForest(
        n_estimators=100,
        contamination=0.05,  # assume ~5% of days are "weird"
        random_state=42,
    )
    model.fit(X_scaled)

    # Save artifacts
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    dump(model, MODEL_PATH)
    dump(scaler, SCALER_PATH)

    print(f"Saved model to {MODEL_PATH}")
    print(f"Saved scaler to {SCALER_PATH}")


if __name__ == "__main__":
    main()


"""
ml_demand_forecast.py — Fase 8: Machine Learning integrado ao pipeline

Modelos:
  1. LinearRegression (scikit-learn) → previsão de demanda 7 dias por categoria
  2. IsolationForest (scikit-learn)  → detecção de anomalias no volume histórico
"""

import os
import warnings
import logging
from datetime import datetime, timezone, timedelta

import polars as pl
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
load_dotenv()

log = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB", "ecommerce"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

FORECAST_HOURS = 7 * 24   # 7 dias em horas
MIN_ROWS       = 3        # mínimo de pontos para treinar

DDL = """
CREATE TABLE IF NOT EXISTS gold.demand_forecast (
    id              BIGSERIAL    PRIMARY KEY,
    category        TEXT         NOT NULL,
    forecast_ts     TIMESTAMPTZ  NOT NULL,
    forecast_date   DATE         NOT NULL,
    forecast_hour   INT          NOT NULL,
    yhat            NUMERIC(10,2),
    yhat_lower      NUMERIC(10,2),
    yhat_upper      NUMERIC(10,2),
    model           TEXT         NOT NULL DEFAULT 'linear_regression',
    generated_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uidx_demand_forecast
    ON gold.demand_forecast (category, forecast_ts);

CREATE TABLE IF NOT EXISTS gold.anomaly_flags (
    id              BIGSERIAL    PRIMARY KEY,
    category        TEXT         NOT NULL,
    order_hour_ts   TIMESTAMPTZ  NOT NULL,
    total_pedidos   INT          NOT NULL,
    anomaly_score   NUMERIC(8,4),
    is_anomaly      BOOLEAN      NOT NULL,
    model           TEXT         NOT NULL DEFAULT 'isolation_forest',
    generated_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uidx_anomaly_flags
    ON gold.anomaly_flags (category, order_hour_ts);
"""

def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    print("[ml] Tabelas verificadas.")

def read_historical_data(conn) -> pl.DataFrame:
    sql = """
        SELECT
            p.category,
            date_trunc('hour', f.order_ts)        AS order_hour_ts,
            COUNT(DISTINCT f.order_id)::int        AS total_pedidos
        FROM gold.fact_orders f
        JOIN gold.dim_product p ON f.product_sk = p.product_sk
        WHERE f.status IN ('completed', 'pending')
          AND f.order_ts IS NOT NULL
        GROUP BY p.category, date_trunc('hour', f.order_ts)
        ORDER BY p.category, order_hour_ts
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    if not rows:
        return pl.DataFrame()
    df = pl.DataFrame({col: [row[i] for row in rows] for i, col in enumerate(cols)})
    print(f"[ml] {len(df)} registros históricos lidos ({df['category'].n_unique()} categorias)")
    return df

def run_linear_regression(df: pl.DataFrame, category: str) -> list[dict]:
    from sklearn.linear_model import LinearRegression
    import numpy as np

    df_cat = df.filter(pl.col("category") == category).sort("order_hour_ts")
    if len(df_cat) < MIN_ROWS:
        print(f"[ml] {category}: apenas {len(df_cat)} pontos — pulando (mínimo {MIN_ROWS})")
        return []

    # Feature: índice temporal (0, 1, 2, ...)
    X = np.arange(len(df_cat)).reshape(-1, 1)
    y = df_cat["total_pedidos"].to_numpy().astype(float)

    model = LinearRegression()
    model.fit(X, y)

    # Resíduos para calcular intervalo de confiança
    y_pred_train = model.predict(X)
    residuals    = y - y_pred_train
    std_residual = residuals.std() if len(residuals) > 1 else 1.0

    # Gera previsões para as próximas FORECAST_HOURS horas
    last_ts   = df_cat["order_hour_ts"][-1]
    rows      = []
    for i in range(1, FORECAST_HOURS + 1):
        x_future   = np.array([[len(df_cat) + i]])
        yhat       = max(0, float(model.predict(x_future)[0]))
        yhat_lower = max(0, yhat - 1.96 * std_residual)
        yhat_upper = yhat + 1.96 * std_residual
        # Calcula o timestamp futuro
        future_ts  = last_ts + timedelta(hours=i)
        if hasattr(future_ts, 'isoformat'):
            ts_str = future_ts.isoformat()
        else:
            ts_str = str(future_ts)

        rows.append({
            "category":      category,
            "forecast_ts":   ts_str,
            "forecast_date": str(future_ts)[:10],
            "forecast_hour": int(future_ts.hour if hasattr(future_ts, 'hour') else i % 24),
            "yhat":          float(round(yhat, 2)),
            "yhat_lower":    float(round(yhat_lower, 2)),
            "yhat_upper":    float(round(yhat_upper, 2)),
            "model":         "linear_regression",
        })

    print(f"[ml] {category}: {len(rows)} previsões geradas com LinearRegression")
    return rows

def run_isolation_forest(df: pl.DataFrame, category: str) -> list[dict]:
    from sklearn.ensemble import IsolationForest
    import numpy as np

    df_cat = df.filter(pl.col("category") == category)
    if len(df_cat) < 3:
        return []

    X      = df_cat["total_pedidos"].to_numpy().reshape(-1, 1)
    model  = IsolationForest(contamination=0.1, random_state=42)
    preds  = model.fit_predict(X)
    scores = model.score_samples(X)

    rows = []
    for i, row in enumerate(df_cat.to_dicts()):
        ts = row["order_hour_ts"]
        rows.append({
            "category":      category,
            "order_hour_ts": ts.isoformat() if hasattr(ts, 'isoformat') else str(ts),
            "total_pedidos": int(row["total_pedidos"]),
            "anomaly_score": round(float(scores[i]), 4),
            "is_anomaly":    bool(preds[i] == -1),
            "model":         "isolation_forest",
        })

    n = sum(1 for r in rows if r["is_anomaly"])
    print(f"[ml] {category}: {n}/{len(rows)} anomalias detectadas")
    return rows

def save_forecasts(conn, rows):
    if not rows:
        return
    sql = """
        INSERT INTO gold.demand_forecast
            (category, forecast_ts, forecast_date, forecast_hour, yhat, yhat_lower, yhat_upper, model)
        VALUES %s
        ON CONFLICT (category, forecast_ts) DO UPDATE SET
            yhat=EXCLUDED.yhat, yhat_lower=EXCLUDED.yhat_lower,
            yhat_upper=EXCLUDED.yhat_upper, generated_at=now()
    """
    data = [(r["category"], r["forecast_ts"], r["forecast_date"],
             r["forecast_hour"], r["yhat"], r["yhat_lower"], r["yhat_upper"], r["model"]) for r in rows]
    with conn.cursor() as cur:
        execute_values(cur, sql, data, page_size=500)
    conn.commit()
    print(f"[ml] {len(rows)} previsões salvas.")

def save_anomalies(conn, rows):
    if not rows:
        return
    sql = """
        INSERT INTO gold.anomaly_flags
            (category, order_hour_ts, total_pedidos, anomaly_score, is_anomaly, model)
        VALUES %s
        ON CONFLICT (category, order_hour_ts) DO UPDATE SET
            anomaly_score=EXCLUDED.anomaly_score, is_anomaly=EXCLUDED.is_anomaly, generated_at=now()
    """
    data = [(r["category"], r["order_hour_ts"], r["total_pedidos"],
             r["anomaly_score"], r["is_anomaly"], r["model"]) for r in rows]
    with conn.cursor() as cur:
        execute_values(cur, sql, data, page_size=500)
    conn.commit()
    print(f"[ml] {len(rows)} anomalias salvas.")

def run_ml_pipeline() -> dict:
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        ensure_tables(conn)
        df = read_historical_data(conn)
        if df.is_empty():
            print("[ml] Sem dados históricos.")
            return {}

        categories      = df["category"].drop_nulls().unique().to_list()
        total_forecasts = 0
        total_anomalies = 0

        for category in categories:
            print(f"\n[ml] Categoria: {category}")
            fc = run_linear_regression(df, category)
            save_forecasts(conn, fc)
            total_forecasts += len(fc)

            an = run_isolation_forest(df, category)
            save_anomalies(conn, an)
            total_anomalies += len(an)

        summary = {"categories": len(categories), "forecasts_saved": total_forecasts,
                   "anomalies_saved": total_anomalies, "run_at": datetime.now(timezone.utc).isoformat()}
        print(f"\n[ml] Resumo: {summary}")
        return summary
    finally:
        conn.close()

if __name__ == "__main__":
    run_ml_pipeline()
"""
dag_silver_transform.py - DAG da camada Silver

Roda logo após a Bronze (sensor) ou a cada hora.
Lê bronze.orders_raw com Polars, aplica as regras de qualidade
e escreve em silver.orders_clean.

Grafo de tarefas:
  start → wait_for_bronze → transform_silver → log_summary → end
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.sql import SqlSensor
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, "/opt/airflow")

DEFAULT_ARGS = {
    "owner":            "pipeline",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

# -------------------------------------------------------------------
# Funções
# -------------------------------------------------------------------

def run_transform(**context):
    from processing.silver_transform import run_silver_transform
    summary = run_silver_transform()
    context["ti"].xcom_push(key="silver_summary", value=summary)
    return summary


def log_summary(**context):
    summary = context["ti"].xcom_pull(task_ids="transform_silver", key="silver_summary")

    print("=" * 55)
    print("  RESUMO — CAMADA SILVER")
    print("=" * 55)
    print(f"  Registros lidos da Bronze    : {summary.get('rows_read', 0)}")
    print(f"  Registros escritos na Silver : {summary.get('rows_written', 0)}")
    print(f"  Registros descartados        : {summary.get('rows_discarded', 0)}")
    if summary.get("rows_read", 0) > 0:
        taxa = summary["rows_discarded"] / summary["rows_read"] * 100
        print(f"  Taxa de descarte             : {taxa:.1f}%")
    print(f"  Último bronze_id processado  : {summary.get('bronze_max_id', '-')}")
    print("=" * 55)
    print()
    print("  Regras de qualidade aplicadas:")
    print("  RQ-01  Duplicatas removidas por order_id")
    print("  RQ-02  Registros com preço nulo descartados")
    print("  RQ-03  Registros com preço negativo descartados")
    print("  RQ-04  Registros com quantity nula ou <= 0 descartados")
    print("  RQ-05  Registros sem customer_id descartados")
    print("  RQ-06  Timestamps no futuro (> now + 1h) descartados")
    print("  RQ-07  Status inválido corrigido para 'unknown'")
    print("  RQ-08  Categorias normalizadas (lowercase, sem acento)")
    print("  RQ-09  total_value recalculado como price * quantity")
    print("  RQ-10  Textos aparados e vazios convertidos para NULL")
    print("=" * 55)

    # Falha se a taxa de descarte for acima de 30% (indica problema no gerador)
    if summary.get("rows_read", 0) > 0:
        taxa = summary["rows_discarded"] / summary["rows_read"] * 100
        if taxa > 30:
            raise ValueError(
                f"Taxa de descarte {taxa:.1f}% acima do limite de 30%. "
                "Verifique o gerador ou as regras de qualidade."
            )


# -------------------------------------------------------------------
# DAG
# -------------------------------------------------------------------
with DAG(
    dag_id="silver_transform",
    description="Transforma bronze.orders_raw em silver.orders_clean com Polars",
    default_args=DEFAULT_ARGS,
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "transformacao", "qualidade", "ecommerce"],
) as dag:

    dag.doc_md = """
    ## DAG: silver_transform

    **Camada Silver — Transformação e qualidade de dados**

    Lê incrementalmente da `bronze.orders_raw` (apenas registros
    com `id` maior que o último processado), aplica 10 regras de
    qualidade com **Polars** e escreve em `silver.orders_clean`.

    ### Regras de qualidade
    | Código | Regra | Ação |
    |--------|-------|------|
    | RQ-01 | Duplicatas por order_id | Descarta |
    | RQ-02 | Preço nulo | Descarta |
    | RQ-03 | Preço negativo | Descarta |
    | RQ-04 | Quantity nula ou ≤ 0 | Descarta |
    | RQ-05 | customer_id nulo | Descarta |
    | RQ-06 | Timestamp no futuro | Descarta |
    | RQ-07 | Status inválido | Corrige → `unknown` |
    | RQ-08 | Categoria | Normaliza (lowercase, sem acento) |
    | RQ-09 | total_value | Recalcula (price × quantity) |
    | RQ-10 | Textos vazios | Converte para NULL |

    ### Tabelas criadas/usadas
    | Tabela | Descrição |
    |--------|-----------|
    | `silver.orders_clean` | Pedidos válidos e normalizados |
    | `silver.transform_log` | Histórico de runs com métricas de qualidade |
    """

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # Sensor: só avança se houver dados novos na Bronze
    wait = SqlSensor(
        task_id="wait_for_bronze",
        conn_id="postgres_pipeline",
        sql="""
            SELECT COUNT(*) FROM bronze.orders_raw
            WHERE id > (
                SELECT COALESCE(MAX(bronze_id), 0)
                FROM silver.orders_clean
            )
        """,
        mode="poke",
        poke_interval=60,   # verifica a cada 60s
        timeout=600,        # desiste após 10 min
        doc_md="Aguarda novos registros na Bronze antes de transformar.",
    )

    transform = PythonOperator(
        task_id="transform_silver",
        python_callable=run_transform,
        doc_md="Aplica as 10 regras de qualidade com Polars e escreve na Silver.",
    )

    summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
        doc_md="Loga métricas de qualidade. Falha se taxa de descarte > 30%.",
    )

    start >> wait >> transform >> summary >> end
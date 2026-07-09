"""
dag_gold_dbt.py — DAG standalone da camada Gold

Útil para reprocessar só a Gold sem rodar o pipeline completo.
Em produção, a Gold é orquestrada pela DAG mestre ecommerce_pipeline.

Grafo: start → dbt_deps → dbt_test_sources → dbt_run → dbt_test_models → end
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner":            "pipeline",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

DBT_DIR = "/opt/airflow/dbt/ecommerce_gold"
DBT_BIN = "/home/airflow/.local/bin/dbt"  # caminho completo — dbt não está no PATH do bash

with DAG(
    dag_id="gold_dbt",
    description="Roda dbt para construir o star schema e KPIs na camada Gold",
    default_args=DEFAULT_ARGS,
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "dbt", "star-schema", "kpi", "ecommerce"],
) as dag:

    dag.doc_md = """
    ## DAG: gold_dbt

    **Camada Gold — Star schema e KPIs com dbt**

    DAG standalone para reprocessar a Gold de forma independente.
    Em uso normal, a Gold é acionada pela DAG `ecommerce_pipeline`.

    ### Modelos (ordem de execução)
    ```
    dim_date, dim_product, dim_customer
        └── fact_orders
                ├── kpi_ticket_medio_por_categoria
                ├── kpi_volume_pedidos_por_hora
                └── kpi_taxa_cancelamento
    ```
    """

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_BIN} deps --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
        doc_md="Instala pacotes dbt (dbt-utils).",
    )

    dbt_test_sources = BashOperator(
        task_id="dbt_test_sources",
        bash_command=f"{DBT_BIN} test --select source:silver --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
        doc_md="Valida que a camada Silver está acessível e tem dados.",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_BIN} run --profiles-dir {DBT_DIR} --project-dir {DBT_DIR} --target dev --no-partial-parse",
        doc_md="Executa todos os modelos: dimensões → fato → KPIs.",
    )

    dbt_test_models = BashOperator(
        task_id="dbt_test_models",
        bash_command=f"{DBT_BIN} test --exclude source:silver --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
        doc_md="Roda testes de unicidade, not_null e accepted_values.",
    )

    start >> dbt_deps >> dbt_test_sources >> dbt_run >> dbt_test_models >> end
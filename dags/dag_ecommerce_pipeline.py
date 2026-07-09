"""
dag_ecommerce_pipeline.py — DAG mestre do pipeline

Orquestra tudo em sequência:
  generate_data → ingest_bronze → process_silver → build_gold

Schedule: a cada 15 minutos
Robustez: retries, alertas de falha via log estruturado, sensor de saúde do banco
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.sql import SqlSensor
from datetime import datetime, timedelta
import sys
import os
import logging

sys.path.insert(0, "/opt/airflow")

log = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Callback de falha — "alerta de produção"
# -------------------------------------------------------------------
def on_failure_callback(context):
    """
    Chamado automaticamente quando qualquer task falha.
    Em produção: substituir o log por chamada à API do Slack,
    PagerDuty, ou smtp.EmailOperator.
    """
    dag_id   = context["dag"].dag_id
    task_id  = context["task_instance"].task_id
    run_id   = context["run_id"]
    exc      = context.get("exception", "sem detalhe")
    log_url  = context["task_instance"].log_url

    log.error("=" * 60)
    log.error("  FALHA NO PIPELINE — ALERTA")
    log.error("=" * 60)
    log.error(f"  DAG     : {dag_id}")
    log.error(f"  Task    : {task_id}")
    log.error(f"  Run ID  : {run_id}")
    log.error(f"  Erro    : {exc}")
    log.error(f"  Log URL : {log_url}")
    log.error("=" * 60)

    # Para adicionar Slack no futuro:
    # from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
    # SlackWebhookOperator(task_id='slack_alert', http_conn_id='slack',
    #     message=f"Pipeline falhou na task {task_id}").execute(context)


def on_success_callback(context):
    dag_id  = context["dag"].dag_id
    run_id  = context["run_id"]
    log.info(f"[{dag_id}] Pipeline concluído com sucesso — run_id: {run_id}")


# -------------------------------------------------------------------
# Default args com retries e callbacks
# -------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner":               "pipeline",
    "depends_on_past":     False,
    "retries":             3,
    "retry_delay":         timedelta(minutes=2),
    "retry_exponential_backoff": True,   # 2min, 4min, 8min
    "max_retry_delay":     timedelta(minutes=10),
    "email_on_failure":    False,        # troque para True e configure SMTP
    "email_on_retry":      False,
    "on_failure_callback": on_failure_callback,
}

# -------------------------------------------------------------------
# Funções das tasks
# -------------------------------------------------------------------

def task_generate_data(**context):
    """Gera um lote de eventos e salva em data/raw/."""
    from ingestion.generator import generate_batch, save_batch, print_summary

    n_events = int(context["params"].get("events_per_run", 50))
    events   = generate_batch(n_events)
    filepath = save_batch(events)
    print_summary(events, filepath)

    context["ti"].xcom_push(key="generated_file", value=str(filepath))
    context["ti"].xcom_push(key="events_count",   value=len(events))
    log.info(f"[generate] {len(events)} eventos salvos em {filepath.name}")
    return len(events)


def task_ingest_bronze(**context):
    """Lê arquivos novos de data/raw/ e insere em bronze.orders_raw."""
    from ingestion.bronze_ingest import run_bronze_ingestion
    summary = run_bronze_ingestion()
    context["ti"].xcom_push(key="bronze_summary", value=summary)

    if summary["files_processed"] == 0 and summary["total_events"] == 0:
        log.warning("[bronze] Nenhum arquivo novo processado.")
    else:
        log.info(f"[bronze] {summary['total_events']} eventos inseridos.")
    return summary


def task_process_silver(**context):
    """Transforma bronze → silver aplicando regras de qualidade."""
    from processing.silver_transform import run_silver_transform
    summary = run_silver_transform()
    context["ti"].xcom_push(key="silver_summary", value=summary)

    taxa = 0
    if summary.get("rows_read", 0) > 0:
        taxa = summary["rows_discarded"] / summary["rows_read"] * 100
        if taxa > 30:
            raise ValueError(
                f"Taxa de descarte {taxa:.1f}% acima do limite (30%). "
                "Verifique a qualidade dos dados na Bronze."
            )
    log.info(f"[silver] {summary.get('rows_written', 0)} registros limpos. Descarte: {taxa:.1f}%")
    return summary


def task_build_gold(**context):
    """Roda dbt run para construir star schema e KPIs na Gold."""
    import subprocess

    dbt_dir = "/opt/airflow/dbt/ecommerce_gold"
    # Caminho completo do dbt (instalado no usuário airflow)
    dbt_bin = "/home/airflow/.local/bin/dbt"
    cmd = [
        dbt_bin, "run",
        "--profiles-dir", dbt_dir,
        "--project-dir", dbt_dir,
        "--target", "dev",
        "--no-partial-parse",
    ]

    log.info(f"[gold] Rodando: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError(f"dbt run falhou:\n{result.stderr}")

    log.info("[gold] dbt run concluído com sucesso.")
    return result.returncode


def task_run_ml(**context):
    """Treina LinearRegression (demanda) e IsolationForest (anomalias). Salva na Gold."""
    from ml.ml_demand_forecast import run_ml_pipeline
    summary = run_ml_pipeline()
    context["ti"].xcom_push(key="ml_summary", value=summary)
    log.info(f"[ml] {summary.get('forecasts_saved', 0)} previsões | {summary.get('anomalies_saved', 0)} anomalias")
    return summary


def task_log_pipeline_summary(**context):
    """Loga resumo completo do pipeline para auditoria."""
    ti = context["ti"]

    gen_count      = ti.xcom_pull(task_ids="generate_data",   key="events_count")   or 0
    bronze_summary = ti.xcom_pull(task_ids="ingest_bronze",   key="bronze_summary") or {}
    silver_summary = ti.xcom_pull(task_ids="process_silver",  key="silver_summary") or {}

    log.info("=" * 60)
    log.info("  RESUMO DO PIPELINE — ecommerce_pipeline")
    log.info("=" * 60)
    log.info(f"  Eventos gerados          : {gen_count}")
    log.info(f"  Arquivos Bronze          : {bronze_summary.get('files_processed', 0)}")
    log.info(f"  Eventos Bronze inseridos : {bronze_summary.get('total_events', 0)}")
    log.info(f"  Registros Silver limpos  : {silver_summary.get('rows_written', 0)}")
    log.info(f"  Registros descartados    : {silver_summary.get('rows_discarded', 0)}")
    ml_summary = ti.xcom_pull(task_ids="run_ml", key="ml_summary") or {}
    log.info(f"  Modelos Gold (dbt)       : 7")
    log.info(f"  Previsões ML geradas     : {ml_summary.get('forecasts_saved', 0)}")
    log.info(f"  Anomalias detectadas     : {ml_summary.get('anomalies_saved', 0)}")
    log.info("=" * 60)


# -------------------------------------------------------------------
# DAG
# -------------------------------------------------------------------
with DAG(
    dag_id="ecommerce_pipeline",
    description="Pipeline completo: generate → bronze → silver → gold",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/15 * * * *",   # a cada 15 minutos
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,                  # evita runs paralelas
    params={"events_per_run": 50},
    on_success_callback=on_success_callback,
    tags=["master", "ecommerce", "pipeline"],
) as dag:

    dag.doc_md = """
    ## DAG: ecommerce_pipeline

    **Pipeline completo end-to-end — E-commerce**

    Roda a cada 15 minutos simulando ingestão near real-time.

    ### Fluxo
    ```
    start
      └── check_db_health         (sensor — banco está ok?)
            └── generate_data     (gera eventos fake)
                  └── ingest_bronze   (JSON → bronze.orders_raw)
                        └── process_silver  (Polars — 10 regras de qualidade)
                              └── build_gold      (dbt run — star schema + KPIs)
                                    └── log_summary
                                          └── end
    ```

    ### Robustez
    - **Retries**: 3 tentativas com backoff exponencial (2→4→8 min)
    - **max_active_runs=1**: sem runs paralelas no mesmo dado
    - **on_failure_callback**: alerta estruturado em caso de falha
    - **SqlSensor**: só avança se o banco estiver saudável
    - **Taxa de descarte**: falha se Silver descartar > 30% dos dados

    ### Parâmetros configuráveis
    | Parâmetro | Padrão | Descrição |
    |-----------|--------|-----------|
    | `events_per_run` | 50 | Eventos gerados por run |
    """

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(
        task_id="end",
        on_success_callback=on_success_callback,
    )

    # Sensor: verifica saúde do banco antes de começar
    check_db = SqlSensor(
        task_id="check_db_health",
        conn_id="postgres_pipeline",
        sql="SELECT 1",
        mode="poke",
        poke_interval=30,
        timeout=120,
        on_failure_callback=on_failure_callback,
        doc_md="Verifica se o PostgreSQL está respondendo antes de qualquer operação.",
    )

    generate = PythonOperator(
        task_id="generate_data",
        python_callable=task_generate_data,
        on_failure_callback=on_failure_callback,
        doc_md="Gera lote de eventos fake com Faker e salva em data/raw/.",
    )

    bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=task_ingest_bronze,
        on_failure_callback=on_failure_callback,
        doc_md="Lê JSONs novos com Polars e insere em bronze.orders_raw sem transformação.",
    )

    silver = PythonOperator(
        task_id="process_silver",
        python_callable=task_process_silver,
        on_failure_callback=on_failure_callback,
        doc_md="Aplica 10 regras de qualidade com Polars. Falha se descarte > 30%.",
    )

    gold = PythonOperator(
        task_id="build_gold",
        python_callable=task_build_gold,
        on_failure_callback=on_failure_callback,
        doc_md="Roda dbt run — constrói dimensões, fato e KPIs no schema gold.",
    )

    ml = PythonOperator(
        task_id="run_ml",
        python_callable=task_run_ml,
        on_failure_callback=on_failure_callback,
        doc_md="Treina LinearRegression (previsão de demanda) e IsolationForest (anomalias). Salva na Gold.",
    )

    summary = PythonOperator(
        task_id="log_summary",
        python_callable=task_log_pipeline_summary,
        doc_md="Loga resumo completo da run para auditoria.",
    )

    # Grafo de dependências
    start >> check_db >> generate >> bronze >> silver >> gold >> ml >> summary >> end
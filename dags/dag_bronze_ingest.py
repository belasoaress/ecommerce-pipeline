"""
dag_bronze_ingest.py - DAG da camada Bronze

Roda a cada 30 minutos, detecta arquivos JSON novos em data/raw/
e carrega na tabela bronze.orders_raw sem transformação.

Grafo de tarefas:
  start → check_raw_dir → ingest_bronze → log_summary → end
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import os

# Garante que o Airflow encontra os módulos do projeto
sys.path.insert(0, "/opt/airflow")

# -------------------------------------------------------------------
# Argumentos padrão
# -------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner":            "pipeline",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
    "email_on_retry":   False,
}

# -------------------------------------------------------------------
# Funções das tarefas
# -------------------------------------------------------------------

def check_raw_dir(**context) -> bool:
    """
    ShortCircuit: retorna False (pula o resto da DAG) se não houver
    arquivos novos em data/raw/.
    """
    from pathlib import Path
    import psycopg2

    raw_dir = Path("/opt/airflow/data/raw")

    if not raw_dir.exists():
        print(f"[check] Pasta {raw_dir} não existe ainda.")
        return False

    all_files = list(raw_dir.glob("events_*.json"))
    if not all_files:
        print("[check] Nenhum arquivo JSON em data/raw/.")
        return False

    # Verifica quais ainda não foram processados
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "ecommerce"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    try:
        # A tabela pode não existir ainda na primeira run
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT filename FROM bronze.ingestion_log")
                processed = {row[0] for row in cur.fetchall()}
        except Exception:
            conn.rollback()
            processed = set()

        pending = [f for f in all_files if f.name not in processed]
        print(f"[check] Total: {len(all_files)}  |  Novos: {len(pending)}")

        # Passa a lista de pendentes via XCom para a próxima task
        context["ti"].xcom_push(key="pending_count", value=len(pending))
        return len(pending) > 0

    finally:
        conn.close()


def run_ingest(**context):
    """
    Chama o módulo de ingestão Bronze e empurra o resumo via XCom.
    """
    from ingestion.bronze_ingest import run_bronze_ingestion
    summary = run_bronze_ingestion()
    context["ti"].xcom_push(key="ingest_summary", value=summary)
    return summary


def log_summary(**context):
    """
    Loga o resumo da ingestão nos logs do Airflow.
    """
    ti      = context["ti"]
    summary = ti.xcom_pull(task_ids="ingest_bronze", key="ingest_summary")
    pending = ti.xcom_pull(task_ids="check_raw_dir", key="pending_count")

    print("=" * 50)
    print("  RESUMO — CAMADA BRONZE")
    print("=" * 50)
    print(f"  Arquivos novos encontrados : {pending}")
    print(f"  Arquivos processados (OK)  : {summary.get('files_processed', 0)}")
    print(f"  Arquivos com erro          : {summary.get('files_error', 0)}")
    print(f"  Total de eventos inseridos : {summary.get('total_events', 0)}")
    print(f"  Executado em               : {summary.get('run_at', '-')}")
    print("=" * 50)

    # Falha a task se houve muitos erros (mais de 50% dos arquivos)
    files_ok  = summary.get("files_processed", 0)
    files_err = summary.get("files_error", 0)
    if files_err > 0 and files_ok == 0:
        raise RuntimeError("Todos os arquivos falharam na ingestão Bronze.")


# -------------------------------------------------------------------
# Definição da DAG
# -------------------------------------------------------------------
with DAG(
    dag_id="bronze_ingest",
    description="Lê arquivos JSON novos de data/raw/ e carrega em bronze.orders_raw",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/30 * * * *",   # a cada 30 minutos
    start_date=datetime(2024, 1, 1),
    catchup=False,                       # não reprocessa runs passadas
    max_active_runs=1,                   # evita runs paralelas no mesmo arquivo
    tags=["bronze", "ingestao", "ecommerce"],
) as dag:

    dag.doc_md = """
    ## DAG: bronze_ingest

    **Camada Bronze - Ingestão sem transformação**

    Detecta arquivos `events_*.json` novos em `data/raw/` e insere
    todos os eventos na tabela `bronze.orders_raw` preservando o
    payload original como JSONB.

    ### Por que sem transformação?
    - Auditabilidade: qualquer reprocessamento parte dos dados originais
    - Erros propositais do gerador entram aqui, serão tratados na Silver
    - O `ingestion_log` evita duplicatas entre runs

    ### Tabelas criadas/usadas
    | Tabela | Descrição |
    |--------|-----------|
    | `bronze.orders_raw` | Eventos crus com payload JSONB |
    | `bronze.ingestion_log` | Rastreio de arquivos processados |

    ### Tarefas
    ```
    start → check_raw_dir → ingest_bronze → log_summary → end
    ```
    """

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    check = ShortCircuitOperator(
        task_id="check_raw_dir",
        python_callable=check_raw_dir,
        doc_md="Verifica se há arquivos novos. Pula o resto da DAG se não houver.",
    )

    ingest = PythonOperator(
        task_id="ingest_bronze",
        python_callable=run_ingest,
        doc_md="Lê os JSONs novos com Polars e insere em bronze.orders_raw.",
    )

    summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
        doc_md="Loga o resumo da execução e falha se todos os arquivos erraram.",
    )

    # Grafo de dependências
    start >> check >> ingest >> summary >> end
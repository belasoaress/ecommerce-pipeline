"""
bronze_ingest.py - Camada Bronze

Lê arquivos JSON novos de data/raw/ e insere na tabela
bronze.orders_raw sem nenhuma transformação.

Princípios:
  - Nada é descartado (nem eventos com erro)
  - Nada é validado (isso é trabalho da Silver)
  - Cada arquivo processado é registrado em bronze.ingestion_log
    para evitar reprocessamento
  - O payload JSON original é preservado inteiro como JSONB
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
# Suporta execução tanto no container pipeline (/app) quanto no Airflow (/opt/airflow)
BASE_DIR = Path(os.getenv('PIPELINE_BASE_DIR', Path(__file__).resolve().parent.parent))
RAW_DIR  = BASE_DIR / "data" / "raw"

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB", "ecommerce"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


# -------------------------------------------------------------------
# Setup das tabelas
# -------------------------------------------------------------------
DDL = """
-- Tabela principal da camada Bronze
CREATE TABLE IF NOT EXISTS bronze.orders_raw (
    id           BIGSERIAL    PRIMARY KEY,
    order_id     TEXT,                        -- pode vir nulo ou duplicado
    event_type   TEXT,
    payload      JSONB        NOT NULL,       -- evento completo exatamente como chegou
    source_file  TEXT         NOT NULL,       -- rastreabilidade: qual arquivo gerou a linha
    ingested_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Índices para consultas de auditoria
CREATE INDEX IF NOT EXISTS idx_orders_raw_order_id
    ON bronze.orders_raw (order_id);
CREATE INDEX IF NOT EXISTS idx_orders_raw_ingested
    ON bronze.orders_raw (ingested_at);
CREATE INDEX IF NOT EXISTS idx_orders_raw_source_file
    ON bronze.orders_raw (source_file);

-- Log de arquivos processados (evita reprocessamento)
CREATE TABLE IF NOT EXISTS bronze.ingestion_log (
    id              BIGSERIAL   PRIMARY KEY,
    filename        TEXT        NOT NULL UNIQUE,
    events_loaded   INT         NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    status          TEXT        NOT NULL DEFAULT 'success'
);
"""


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    print("[bronze] Tabelas verificadas/criadas.")


# -------------------------------------------------------------------
# Arquivos novos (não processados ainda)
# -------------------------------------------------------------------
def get_pending_files(conn) -> list[Path]:
    """Retorna arquivos JSON em data/raw/ que ainda não foram processados."""
    with conn.cursor() as cur:
        cur.execute("SELECT filename FROM bronze.ingestion_log")
        processed = {row[0] for row in cur.fetchall()}

    all_files = sorted(RAW_DIR.glob("events_*.json"))
    pending   = [f for f in all_files if f.name not in processed]

    print(f"[bronze] Arquivos em raw/: {len(all_files)}  |  Novos: {len(pending)}")
    return pending


# -------------------------------------------------------------------
# Ingestão de um arquivo
# -------------------------------------------------------------------
def ingest_file(conn, filepath: Path) -> int:
    """
    Lê um JSON com Polars, monta os registros e insere em bronze.orders_raw.
    Retorna a quantidade de eventos inseridos.
    """
    # Polars lê o JSON
    df = pl.read_json(filepath)
    print(f"[bronze] {filepath.name} → {len(df)} eventos lidos com Polars")

    # Monta lista de tuplas para INSERT em lote
    # O payload é o evento inteiro serializado de volta como JSON string → JSONB
    rows = []
    for row in df.to_dicts():
        rows.append((
            row.get("order_id"),           # pode ser None 
            row.get("status", "unknown"),  # usado como event_type para categorização
            json.dumps(row, ensure_ascii=False, default=str),  # payload completo
            filepath.name,                 # rastreabilidade
        ))

    if not rows:
        print(f"[bronze] {filepath.name} → arquivo vazio, pulando.")
        return 0

    # INSERT em lote com psycopg2 (rápido para muitas linhas)
    insert_sql = """
        INSERT INTO bronze.orders_raw (order_id, event_type, payload, source_file)
        VALUES %s
    """
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, rows, page_size=500)

    conn.commit()
    return len(rows)


# -------------------------------------------------------------------
# Log de arquivo processado
# -------------------------------------------------------------------
def mark_as_processed(conn, filename: str, events_loaded: int, status: str = "success"):
    sql = """
        INSERT INTO bronze.ingestion_log (filename, events_loaded, status)
        VALUES (%s, %s, %s)
        ON CONFLICT (filename) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(sql, (filename, events_loaded, status))
    conn.commit()


# -------------------------------------------------------------------
# Entrypoint principal (chamado pela DAG e também direto pela CLI)
# -------------------------------------------------------------------
def run_bronze_ingestion() -> dict:
    """
    Processa todos os arquivos novos.
    Retorna dict com resumo para o Airflow usar como XCom.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        ensure_tables(conn)
        pending = get_pending_files(conn)

        if not pending:
            print("[bronze] Nenhum arquivo novo. Nada a fazer.")
            return {"files_processed": 0, "total_events": 0}

        total_events = 0
        files_ok     = 0
        files_error  = 0

        for filepath in pending:
            try:
                n = ingest_file(conn, filepath)
                mark_as_processed(conn, filepath.name, n, status="success")
                total_events += n
                files_ok     += 1
                print(f"[bronze] OK — {filepath.name} ({n} eventos)")

            except Exception as exc:
                # Registra o erro mas continua processando os demais arquivos
                conn.rollback()
                mark_as_processed(conn, filepath.name, 0, status=f"error: {exc}")
                files_error += 1
                print(f"[bronze] ERRO — {filepath.name}: {exc}")

        summary = {
            "files_processed": files_ok,
            "files_error":     files_error,
            "total_events":    total_events,
            "run_at":          datetime.now(timezone.utc).isoformat(),
        }
        print(f"\n[bronze] Resumo: {summary}")
        return summary

    finally:
        conn.close()


if __name__ == "__main__":
    run_bronze_ingestion()
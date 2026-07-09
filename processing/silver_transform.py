"""
silver_transform.py — Camada Silver

Lê de bronze.orders_raw, aplica todas as regras de qualidade
com Polars e escreve em silver.orders_clean.

Regras de qualidade aplicadas (documentadas para README):
──────────────────────────────────────────────────────────────────
RQ-01  Remoção de duplicatas por order_id (mantém o registro mais recente)
RQ-02  Preço nulo → descartado (sem preço não há pedido válido)
RQ-03  Preço negativo → descartado
RQ-04  Quantidade nula ou <= 0 → descartada
RQ-05  customer_id nulo → descartado
RQ-06  Timestamp no futuro (> now + 1h) → descartado
RQ-07  Status fora do domínio {completed, pending, cancelled} → corrigido para "unknown"
RQ-08  Categoria normalizada: lowercase, sem acentos, sem espaços extras
RQ-09  total_value recalculado como price * quantity (não confia no valor do JSON)
RQ-10  Campos de texto aparados (strip) e vazios → None
──────────────────────────────────────────────────────────────────
"""

import os
import unicodedata
from datetime import datetime, timezone, timedelta

import polars as pl
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB", "ecommerce"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

VALID_STATUSES  = {"completed", "pending", "cancelled"}
MAX_FUTURE_SECS = 3600  # timestamps até 1h no futuro são tolerados


# -------------------------------------------------------------------
# DDL — cria tabelas Silver (idempotente)
# -------------------------------------------------------------------
DDL = """
CREATE TABLE IF NOT EXISTS silver.orders_clean (
    id              BIGSERIAL    PRIMARY KEY,
    order_id        TEXT         NOT NULL,
    customer_id     TEXT         NOT NULL,
    customer_name   TEXT,
    customer_email  TEXT,
    region          TEXT,
    city            TEXT,
    product_id      TEXT         NOT NULL,
    product_name    TEXT,
    category        TEXT,
    price           NUMERIC(10,2) NOT NULL,
    quantity        INT           NOT NULL,
    total_value     NUMERIC(10,2) NOT NULL,
    payment_method  TEXT,
    installments    INT,
    status          TEXT          NOT NULL,
    order_ts        TIMESTAMPTZ,
    source_file     TEXT,
    bronze_id       BIGINT,                     -- referência ao registro de origem na Bronze
    loaded_at       TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_orders_clean_order_id
    ON silver.orders_clean (order_id);          -- garante unicidade na Silver

CREATE INDEX IF NOT EXISTS idx_orders_clean_status
    ON silver.orders_clean (status);
CREATE INDEX IF NOT EXISTS idx_orders_clean_category
    ON silver.orders_clean (category);
CREATE INDEX IF NOT EXISTS idx_orders_clean_ts
    ON silver.orders_clean (order_ts);

-- Controle de runs da Silver
CREATE TABLE IF NOT EXISTS silver.transform_log (
    id                  BIGSERIAL    PRIMARY KEY,
    bronze_max_id       BIGINT       NOT NULL,   -- último bronze.id processado nesta run
    rows_read           INT          NOT NULL,
    rows_written        INT          NOT NULL,
    rows_discarded      INT          NOT NULL,
    discard_detail      JSONB,                   -- contagem por regra de qualidade
    run_at              TIMESTAMPTZ  NOT NULL DEFAULT now()
);
"""


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    print("[silver] Tabelas verificadas/criadas.")


# -------------------------------------------------------------------
# Leitura incremental da Bronze
# -------------------------------------------------------------------
def get_last_bronze_id(conn) -> int:
    """Retorna o último bronze_id já processado (0 se nunca rodou)."""
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(bronze_id) FROM silver.orders_clean")
        result = cur.fetchone()[0]
    return result or 0


def read_bronze(conn, after_id: int) -> pl.DataFrame:
    """
    Lê bronze.orders_raw com Polars via psycopg2.
    Extrai cada campo do JSONB payload individualmente.
    """
    sql = """
        SELECT
            id                              AS bronze_id,
            payload->>'order_id'            AS order_id,
            payload->>'customer_id'         AS customer_id,
            payload->>'customer_name'       AS customer_name,
            payload->>'customer_email'      AS customer_email,
            payload->>'region'              AS region,
            payload->>'city'                AS city,
            payload->>'product_id'          AS product_id,
            payload->>'product_name'        AS product_name,
            payload->>'category'            AS category,
            payload->>'price'               AS price_raw,
            payload->>'quantity'            AS quantity_raw,
            payload->>'total_value'         AS total_value_raw,
            payload->>'payment_method'      AS payment_method,
            payload->>'installments'        AS installments_raw,
            payload->>'status'              AS status,
            payload->>'timestamp'           AS ts_raw,
            source_file
        FROM bronze.orders_raw
        WHERE id > %s
        ORDER BY id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (after_id,))
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    if not rows:
        return pl.DataFrame()

    df = pl.DataFrame(
        {col: [row[i] for row in rows] for i, col in enumerate(cols)}
    )
    print(f"[silver] Lidos {len(df)} registros da Bronze (após id={after_id})")
    return df


# -------------------------------------------------------------------
# Helpers de normalização
# -------------------------------------------------------------------
def remove_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


def normalize_category(cat) -> object:
    if cat is None:
        return None
    return remove_accents(cat.strip().lower())


# -------------------------------------------------------------------
# Transformações com Polars
# -------------------------------------------------------------------
def apply_quality_rules(df: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    """
    Aplica todas as regras de qualidade e retorna:
      - DataFrame limpo
      - Dict com contagem de descartes por regra
    """
    initial = len(df)
    discards = {}

    # RQ-10 — Strip e vazios → None (antes de qualquer outra regra)
    for col in ["order_id", "customer_id", "status", "category",
                "customer_name", "customer_email", "region", "city",
                "product_id", "product_name", "payment_method"]:
        if col in df.columns:
            df = df.with_columns(
                pl.when(pl.col(col).str.strip_chars() == "")
                  .then(None)
                  .otherwise(pl.col(col).str.strip_chars())
                  .alias(col)
            )

    # Tipagem: numéricos
    df = df.with_columns([
        pl.col("price_raw").cast(pl.Float64, strict=False).alias("price"),
        pl.col("quantity_raw").cast(pl.Int64,   strict=False).alias("quantity"),
        pl.col("total_value_raw").cast(pl.Float64, strict=False).alias("total_value_raw_f"),
        pl.col("installments_raw").cast(pl.Int64, strict=False).alias("installments"),
    ])

    # RQ-02 — Preço nulo
    mask = df["price"].is_null()
    discards["RQ-02_preco_nulo"] = int(mask.sum())
    df = df.filter(~mask)

    # RQ-03 — Preço negativo
    mask = df["price"] < 0
    discards["RQ-03_preco_negativo"] = int(mask.sum())
    df = df.filter(~mask)

    # RQ-04 — Quantidade nula ou <= 0
    mask = df["quantity"].is_null() | (df["quantity"] <= 0)
    discards["RQ-04_quantidade_invalida"] = int(mask.sum())
    df = df.filter(~mask)

    # RQ-05 — customer_id nulo
    mask = df["customer_id"].is_null()
    discards["RQ-05_customer_nulo"] = int(mask.sum())
    df = df.filter(~mask)

    # RQ-06 — Timestamp: parse e validação
    df = df.with_columns(
        pl.col("ts_raw")
          .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False, ambiguous="earliest")
          .alias("order_ts")
    )
    now_utc = datetime.now(timezone.utc)
    max_ts  = now_utc + timedelta(seconds=MAX_FUTURE_SECS)

    # Compara usando filter com expressão Polars pura (evita misturar Series com Expression)
    before_rq06 = len(df)
    df = df.filter(
        pl.col("order_ts").is_null() |
        (pl.col("order_ts") <= pl.lit(max_ts))
    )
    discards["RQ-06_timestamp_futuro"] = before_rq06 - len(df)

    # RQ-07 — Status nulo ou fora do domínio → corrige para "unknown" (não descarta)
    invalid_status = df["status"].is_null() | ~df["status"].is_in(list(VALID_STATUSES))
    discards["RQ-07_status_corrigido"] = int(invalid_status.sum())
    df = df.with_columns(
        pl.when(pl.col("status").is_null() | ~pl.col("status").is_in(list(VALID_STATUSES)))
          .then(pl.lit("unknown"))
          .otherwise(pl.col("status"))
          .alias("status")
    )

    # RQ-08 — Normaliza categoria
    df = df.with_columns(
        pl.col("category")
          .map_elements(normalize_category, return_dtype=pl.Utf8)
          .alias("category")
    )
    discards["RQ-08_categoria_normalizada"] = int(
        df["category"].is_not_null().sum()
    )

    # RQ-09 — Recalcula total_value com base em price * quantity
    df = df.with_columns(
        (pl.col("price") * pl.col("quantity")).round(2).alias("total_value")
    )

    # RQ-01 — Remove duplicatas por order_id (mantém o de maior bronze_id = mais recente)
    before_dedup = len(df)
    df = df.sort("bronze_id", descending=True).unique(subset=["order_id"], keep="first")
    discards["RQ-01_duplicatas"] = before_dedup - len(df)

    total_discarded = initial - len(df)
    discards["_total_descartados"] = total_discarded
    discards["_total_lidos"]       = initial
    discards["_total_escritos"]    = len(df)

    print(f"[silver] Regras aplicadas → lidos: {initial} | escritos: {len(df)} | descartados: {total_discarded}")
    for rule, count in discards.items():
        if not rule.startswith("_") and count > 0:
            print(f"         {rule}: {count}")

    return df, discards


# -------------------------------------------------------------------
# Escrita na Silver
# -------------------------------------------------------------------
def write_silver(conn, df: pl.DataFrame):
    """Insere em silver.orders_clean, ignorando order_ids já existentes."""
    if df.is_empty():
        return

    COLUMNS = [
        "order_id", "customer_id", "customer_name", "customer_email",
        "region", "city", "product_id", "product_name", "category",
        "price", "quantity", "total_value", "payment_method",
        "installments", "status", "order_ts", "source_file", "bronze_id",
    ]

    rows = []
    for row in df.to_dicts():
        rows.append(tuple(row.get(c) for c in COLUMNS))

    sql = f"""
        INSERT INTO silver.orders_clean
            ({', '.join(COLUMNS)})
        VALUES %s
        ON CONFLICT (order_id) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)
    conn.commit()
    print(f"[silver] {len(rows)} registros enviados ao banco.")


# -------------------------------------------------------------------
# Log de run
# -------------------------------------------------------------------
def log_run(conn, bronze_max_id: int, discards: dict):
    import json
    sql = """
        INSERT INTO silver.transform_log
            (bronze_max_id, rows_read, rows_written, rows_discarded, discard_detail)
        VALUES (%s, %s, %s, %s, %s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            bronze_max_id,
            discards.get("_total_lidos", 0),
            discards.get("_total_escritos", 0),
            discards.get("_total_descartados", 0),
            json.dumps({k: v for k, v in discards.items() if not k.startswith("_")}),
        ))
    conn.commit()


# -------------------------------------------------------------------
# Entrypoint
# -------------------------------------------------------------------
def run_silver_transform() -> dict:
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        ensure_tables(conn)

        last_id = get_last_bronze_id(conn)
        df_raw  = read_bronze(conn, after_id=last_id)

        if df_raw.is_empty():
            print("[silver] Nenhum dado novo na Bronze. Nada a fazer.")
            return {"rows_read": 0, "rows_written": 0, "rows_discarded": 0}

        bronze_max_id   = int(df_raw["bronze_id"].max())
        df_clean, discards = apply_quality_rules(df_raw)

        write_silver(conn, df_clean)
        log_run(conn, bronze_max_id, discards)

        return {
            "rows_read":      discards["_total_lidos"],
            "rows_written":   discards["_total_escritos"],
            "rows_discarded": discards["_total_descartados"],
            "bronze_max_id":  bronze_max_id,
        }
    finally:
        conn.close()


if __name__ == "__main__":
    run_silver_transform()
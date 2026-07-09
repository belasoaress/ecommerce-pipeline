"""
generator.py — Gerador de eventos de e-commerce (camada Bronze)

Salva lotes de eventos como JSON em data/raw/.
A camada Bronze do pipeline vai ler esses arquivos.

Uso:
  python ingestion/generator.py                         # 1 lote de 20 eventos e sai
  python ingestion/generator.py --events 50             # 1 lote de 50 eventos
  python ingestion/generator.py --loop --interval 60    # loop a cada 60s
  python ingestion/generator.py --loop --interval 30 --events 100
"""

import json
import random
import argparse
import time
import os
from datetime import datetime, timezone
from pathlib import Path
from faker import Faker

fake = Faker("pt_BR")
random.seed()

# -------------------------------------------------------------------
# Caminhos
# -------------------------------------------------------------------
# Suporta execução tanto no container pipeline (/app) quanto no Airflow (/opt/airflow)
BASE_DIR = Path(os.getenv('PIPELINE_BASE_DIR', Path(__file__).resolve().parent.parent))
RAW_DIR  = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------------------------------------------------
# Catálogo de produtos com pesos de popularidade
# -------------------------------------------------------------------
PRODUCTS = [
    # (product_id, name, category, base_price, peso_popularidade)
    ("P001", "Tenis Runner Pro",       "calcados",    299.90, 8),
    ("P002", "Camiseta Dry Fit",       "vestuario",    89.90, 12),
    ("P003", "Mochila Urbana 30L",     "acessorios",  189.90, 6),
    ("P004", "Fone Bluetooth Max",     "eletronicos", 459.90, 9),
    ("P005", "Relogio Smart Fit",      "eletronicos", 799.90, 5),
    ("P006", "Garrafa Termica 1L",     "acessorios",   69.90, 10),
    ("P007", "Shorts Academia",        "vestuario",    59.90, 11),
    ("P008", "Suplemento Whey 1kg",    "nutricao",    149.90, 7),
    ("P009", "Tenis Casual Slip",      "calcados",    179.90, 9),
    ("P010", "Legging Compressao",     "vestuario",   119.90, 13),
    ("P011", "Barra de Proteina 12un", "nutricao",     79.90, 8),
    ("P012", "Meia Esportiva Kit 3",   "acessorios",   39.90, 14),
    ("P013", "Jaqueta Corta-Vento",    "vestuario",   249.90, 4),
    ("P014", "Tapete de Yoga 6mm",     "acessorios",   99.90, 6),
    ("P015", "Camera de Acao 4K",      "eletronicos", 999.90, 3),
]

PRODUCT_IDS   = [p[0] for p in PRODUCTS]
PRODUCT_NAMES = {p[0]: p[1] for p in PRODUCTS}
CATEGORIES    = {p[0]: p[2] for p in PRODUCTS}
BASE_PRICES   = {p[0]: p[3] for p in PRODUCTS}
POP_WEIGHTS   = [p[4] for p in PRODUCTS]

PAYMENT_METHODS = ["cartao_credito", "pix", "boleto", "cartao_debito"]
PAYMENT_WEIGHTS = [40, 35, 15, 10]

REGIONS = ["sudeste", "sul", "nordeste", "centro_oeste", "norte"]

STATUS_MAP = {"completed": 70, "pending": 20, "cancelled": 10}

# -------------------------------------------------------------------
# Variacao de volume por hora do dia
# Picos: manha (9-11h), almoco (12-13h), noite (20-22h)
# -------------------------------------------------------------------
HOUR_WEIGHTS = {
    0: 1,  1: 1,  2: 1,  3: 1,  4: 1,
    5: 2,  6: 3,  7: 4,  8: 5,
    9: 9,  10: 10, 11: 9,
    12: 8, 13: 7,
    14: 5, 15: 5, 16: 6, 17: 6, 18: 7, 19: 8,
    20: 10, 21: 10, 22: 8,
    23: 3,
}

def volume_multiplier() -> float:
    hour = datetime.now().hour
    peak = max(HOUR_WEIGHTS.values())
    return HOUR_WEIGHTS.get(hour, 5) / peak


# -------------------------------------------------------------------
# Erros intencionais para limpeza na camada Silver
# -------------------------------------------------------------------
ERROR_RATE = 0.08  # 8% dos eventos

def inject_error(event: dict) -> dict:
    error_type = random.choice([
        "null_price",
        "negative_price",
        "null_quantity",
        "zero_quantity",
        "null_customer",
        "future_timestamp",
        "invalid_status",
        "missing_category",
        "duplicate_order_id",
    ])

    if error_type == "null_price":
        event["price"] = None
        event["_error"] = "null_price"
    elif error_type == "negative_price":
        event["price"] = round(-abs(event.get("price", 10)), 2)
        event["_error"] = "negative_price"
    elif error_type == "null_quantity":
        event["quantity"] = None
        event["_error"] = "null_quantity"
    elif error_type == "zero_quantity":
        event["quantity"] = 0
        event["_error"] = "zero_quantity"
    elif error_type == "null_customer":
        event["customer_id"] = None
        event["_error"] = "null_customer"
    elif error_type == "future_timestamp":
        event["timestamp"] = "2099-01-01T12:00:00+00:00"
        event["_error"] = "future_timestamp"
    elif error_type == "invalid_status":
        event["status"] = random.choice(["ERRO", "unknown", "", "processando"])
        event["_error"] = "invalid_status"
    elif error_type == "missing_category":
        event["category"] = None
        event["_error"] = "missing_category"
    elif error_type == "duplicate_order_id":
        event["order_id"] = "ORD00000001"
        event["_error"] = "duplicate_order_id"

    return event


# -------------------------------------------------------------------
# Geracao de evento
# -------------------------------------------------------------------
def make_event() -> dict:
    product_id = random.choices(PRODUCT_IDS, weights=POP_WEIGHTS, k=1)[0]
    price      = round(BASE_PRICES[product_id] * random.uniform(0.95, 1.05), 2)
    quantity   = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 12, 8, 5], k=1)[0]

    event = {
        "order_id":       f"ORD{fake.numerify('########')}",
        "customer_id":    f"C{fake.numerify('######')}",
        "customer_name":  fake.name(),
        "customer_email": fake.email(),
        "region":         random.choice(REGIONS),
        "city":           fake.city(),
        "product_id":     product_id,
        "product_name":   PRODUCT_NAMES[product_id],
        "category":       CATEGORIES[product_id],
        "price":          price,
        "quantity":       quantity,
        "total_value":    round(price * quantity, 2),
        "payment_method": random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS, k=1)[0],
        "installments":   random.choices([1, 1, 2, 3, 6, 12], weights=[40, 20, 15, 10, 10, 5], k=1)[0],
        "status":         random.choices(
                              list(STATUS_MAP.keys()),
                              weights=list(STATUS_MAP.values()), k=1
                          )[0],
        "timestamp":      datetime.now(timezone.utc).isoformat(),
    }

    if random.random() < ERROR_RATE:
        event = inject_error(event)

    return event


# -------------------------------------------------------------------
# Lote
# -------------------------------------------------------------------
def generate_batch(n_events: int) -> list:
    multiplier = volume_multiplier()
    adjusted_n = max(1, int(n_events * multiplier))
    return [make_event() for _ in range(adjusted_n)]


def save_batch(events: list) -> Path:
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = RAW_DIR / f"events_{ts}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=2, default=str)
    return filename


def print_summary(events: list, filepath: Path):
    total   = len(events)
    errors  = sum(1 for e in events if "_error" in e)
    statuses = {}
    cats     = {}
    for e in events:
        s = e.get("status", "?")
        statuses[s] = statuses.get(s, 0) + 1
        c = e.get("category") or "null"
        cats[c] = cats.get(c, 0) + 1

    ts = datetime.now().strftime("%H:%M:%S")
    print(f"\n[{ts}] Lote salvo -> {filepath.name}")
    print(f"  Eventos : {total}  |  Com erro: {errors} ({errors/total*100:.1f}%)")
    print(f"  Status  : { ' | '.join(f'{k}: {v}' for k,v in statuses.items()) }")
    top_cats = sorted(cats.items(), key=lambda x: -x[1])[:3]
    print(f"  Top cats: { ' | '.join(f'{k}: {v}' for k,v in top_cats) }")


# -------------------------------------------------------------------
# Entrypoint
# -------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Gerador de eventos Bronze")
    parser.add_argument("--events",   type=int, default=20,
                        help="Eventos base por lote (ajustado pelo horario). Padrao: 20")
    parser.add_argument("--loop",     action="store_true",
                        help="Gera em loop continuo")
    parser.add_argument("--interval", type=int, default=60,
                        help="Segundos entre lotes no modo loop. Padrao: 60")
    args = parser.parse_args()

    print(f"[generator] Salvando em: {RAW_DIR}")
    print(f"[generator] Taxa de erros: {ERROR_RATE*100:.0f}%")

    def run_once():
        events   = generate_batch(args.events)
        filepath = save_batch(events)
        print_summary(events, filepath)

    if args.loop:
        print(f"[generator] Modo loop — lote a cada {args.interval}s. Ctrl+C para parar.\n")
        while True:
            run_once()
            time.sleep(args.interval)
    else:
        run_once()
        print(f"\n[generator] Concluido. Use --loop para geracao continua.")


if __name__ == "__main__":
    main()
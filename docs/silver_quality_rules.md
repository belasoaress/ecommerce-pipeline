# Regras de Qualidade — Camada Silver

> Documento gerado para compor o README do projeto e servir de base
> para o post no LinkedIn.

---

## Regras aplicadas

### RQ-01 — Remoção de duplicatas
- **Campo:** `order_id`
- **Problema:** o gerador pode emitir o mesmo `order_id` mais de uma vez
  (simulando retransmissão de eventos ou falha de rede)
- **Ação:** mantém apenas o registro com maior `bronze_id` (mais recente)
- **Implementação Polars:**
  ```python
  df.sort("bronze_id", descending=True).unique(subset=["order_id"], keep="first")
  ```

---

### RQ-02 — Preço nulo
- **Campo:** `price`
- **Problema:** campo ausente ou explicitamente `null` no JSON
- **Ação:** descarta o registro — pedido sem preço não tem valor analítico
- **Implementação Polars:**
  ```python
  df.filter(pl.col("price").is_not_null())
  ```

---

### RQ-03 — Preço negativo
- **Campo:** `price`
- **Problema:** preço negativo é impossível no domínio de e-commerce
- **Ação:** descarta o registro
- **Implementação Polars:**
  ```python
  df.filter(pl.col("price") >= 0)
  ```

---

### RQ-04 — Quantidade inválida
- **Campo:** `quantity`
- **Problema:** `null`, zero ou negativo — todos inválidos para um pedido
- **Ação:** descarta o registro
- **Implementação Polars:**
  ```python
  df.filter(pl.col("quantity").is_not_null() & (pl.col("quantity") > 0))
  ```

---

### RQ-05 — Customer ID nulo
- **Campo:** `customer_id`
- **Problema:** sem identificação do cliente não é possível calcular
  métricas como LTV, recorrência ou abandono de carrinho
- **Ação:** descarta o registro
- **Implementação Polars:**
  ```python
  df.filter(pl.col("customer_id").is_not_null())
  ```

---

### RQ-06 — Timestamp no futuro
- **Campo:** `timestamp`
- **Problema:** eventos com data futura indicam erro de relógio do sistema
  emissor ou manipulação de dados; distorcem análises de série temporal
- **Tolerância:** até 1 hora no futuro (margem para fuso horário / clock skew)
- **Ação:** descarta o registro
- **Implementação Polars:**
  ```python
  df.filter(pl.col("order_ts") <= (datetime.now(utc) + timedelta(hours=1)))
  ```

---

### RQ-07 — Status inválido
- **Campo:** `status`
- **Domínio válido:** `{completed, pending, cancelled}`
- **Problema:** valores como `"ERRO"`, `"processando"`, `""` chegam do gerador
- **Decisão de negócio:** não descartar — o pedido em si pode ser válido;
  apenas o status é desconhecido
- **Ação:** corrige para `"unknown"` (mantém o registro)
- **Implementação Polars:**
  ```python
  df.with_columns(
      pl.when(~pl.col("status").is_in(["completed","pending","cancelled"]))
        .then(pl.lit("unknown"))
        .otherwise(pl.col("status"))
        .alias("status")
  )
  ```

---

### RQ-08 — Normalização de categoria
- **Campo:** `category`
- **Problema:** variações como `"Eletrônicos"`, `"eletronico"`, `"ELETRONICOS"`
  geram múltiplos grupos na análise quando são o mesmo segmento
- **Ação:** lowercase + remoção de acentos + strip de espaços
- **Resultado:** `"eletronicos"`, `"vestuario"`, `"calcados"`, etc.
- **Implementação:**
  ```python
  def normalize_category(cat):
      return unicodedata.normalize("NFD", cat.strip().lower())
             .encode("ascii", "ignore").decode()
  ```

---

### RQ-09 — Recálculo de total_value
- **Campos:** `price`, `quantity`, `total_value`
- **Problema:** o `total_value` do JSON pode estar desatualizado se houve
  reprocessamento ou é calculado incorretamente pelo sistema emissor
- **Decisão:** não confiar no valor recebido; sempre recalcular
- **Ação:** `total_value = price * quantity` (arredondado em 2 casas)
- **Implementação Polars:**
  ```python
  df.with_columns(
      (pl.col("price") * pl.col("quantity")).round(2).alias("total_value")
  )
  ```

---

### RQ-10 — Textos vazios → NULL
- **Campos:** todos os campos de texto
- **Problema:** strings `""` e `"  "` são armazenadas como dado presente,
  mas semanticamente são ausência de informação
- **Ação:** strip + conversão de strings vazias para `NULL`
- **Implementação Polars:**
  ```python
  pl.when(pl.col(c).str.strip_chars() == "")
    .then(None)
    .otherwise(pl.col(c).str.strip_chars())
  ```

---

## Métricas de qualidade (exemplo de run)

| Métrica | Valor típico |
|---------|-------------|
| Registros lidos da Bronze | 100 |
| Registros escritos na Silver | ~88 |
| Taxa de descarte esperada | 8–12% |
| Limite de alerta (DAG falha) | > 30% |

> A taxa de ~8% é proposital — o gerador injeta erros em 8% dos eventos
> para simular dados do mundo real.

---
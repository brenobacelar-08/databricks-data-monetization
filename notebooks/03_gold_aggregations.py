# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: Modelo Dimensional e Agregações de BI
# MAGIC
# MAGIC **O que este notebook faz:**
# MAGIC - Lê da Silver (Delta), monta a camada Gold com:
# MAGIC   - `dim_geography` — dimensão geográfica (cidade, UF, região)
# MAGIC   - `fct_events` — tabela fato com métricas de negócio por cidade/UF
# MAGIC   - `agg_by_region` — agregação resumida por região (para KPIs de alto nível)
# MAGIC
# MAGIC **Personalize os nomes de colunas e métricas para o seu domínio de negócio.**

# COMMAND ----------

# MAGIC %md ## Configuração

# COMMAND ----------

dbutils.widgets.text("catalog",       "data_monetization", "Catalog")
dbutils.widgets.text("silver_table",  "events",            "Tabela Silver (origem)")
dbutils.widgets.text("col_metric",    "value",             "Coluna numérica principal")
dbutils.widgets.text("col_date",      "event_date",        "Coluna de data/timestamp")
dbutils.widgets.text("col_state",     "state",             "Coluna UF")
dbutils.widgets.text("col_city",      "city",              "Coluna cidade")

CATALOG       = dbutils.widgets.get("catalog")
SILVER_TABLE  = f"{CATALOG}.silver.{dbutils.widgets.get('silver_table')}"
GOLD_SCHEMA   = f"{CATALOG}.gold"
COL_METRIC    = dbutils.widgets.get("col_metric")
COL_DATE      = dbutils.widgets.get("col_date")
COL_STATE     = dbutils.widgets.get("col_state")
COL_CITY      = dbutils.widgets.get("col_city")

print(f"Origem : {SILVER_TABLE}")
print(f"Destino: {GOLD_SCHEMA}.*")

# COMMAND ----------

from pyspark.sql import functions as F

df_silver = spark.table(SILVER_TABLE)
print(f"Linhas na Silver: {df_silver.count()}")

# COMMAND ----------

# MAGIC %md ## 1. Dimensão Geográfica — `dim_geography`
# MAGIC
# MAGIC Tabela de dimensão: um registro por combinação única de cidade+UF+região.
# MAGIC Recebe uma surrogate key `geo_sk` para uso no modelo estrela.

# COMMAND ----------

dim_geography = (
    df_silver
    .select(COL_CITY, COL_STATE, "region")
    .distinct()
    .withColumn("geo_sk", F.monotonically_increasing_id())
    .orderBy(COL_STATE, COL_CITY)
)

(
    dim_geography.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_geography")
)

print("dim_geography gravada.")
dim_geography.display()

# COMMAND ----------

# MAGIC %md ## 2. Fato — `fct_events`
# MAGIC
# MAGIC Tabela fato: métricas numéricas por data + localização.
# MAGIC Ajuste `col_metric` no widget para a coluna numérica do seu CSV (ex: valor, quantidade, cliques).

# COMMAND ----------

# Enriquece o Silver com a surrogate key geográfica
df_with_geo_sk = (
    df_silver
    .join(
        dim_geography.select("geo_sk", COL_CITY, COL_STATE),
        on=[COL_CITY, COL_STATE],
        how="left"
    )
)

# Extrai partes de data (se a coluna existir no CSV)
if COL_DATE in df_silver.columns:
    df_with_geo_sk = (
        df_with_geo_sk
        .withColumn("event_year",  F.year(F.col(COL_DATE)))
        .withColumn("event_month", F.month(F.col(COL_DATE)))
        .withColumn("event_day",   F.dayofmonth(F.col(COL_DATE)))
    )

# Seleciona colunas finais para a fato (apenas o que o BI precisa)
select_cols = ["geo_sk", COL_DATE, COL_STATE, COL_CITY, "region", "has_valid_coordinates"]
if COL_METRIC in df_silver.columns:
    select_cols.append(COL_METRIC)
if "event_year" in df_with_geo_sk.columns:
    select_cols += ["event_year", "event_month", "event_day"]

fct_events = df_with_geo_sk.select(*[c for c in select_cols if c in df_with_geo_sk.columns])

(
    fct_events.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.fct_events")
)

print("fct_events gravada.")
fct_events.display()

# COMMAND ----------

# MAGIC %md ## 3. Agregação por Região — `agg_by_region`
# MAGIC
# MAGIC Tabela pré-agregada: alimenta KPI cards e gráficos de barras no dashboard.
# MAGIC É o que você vai usar para mostrar "presença por região" ao gestor.

# COMMAND ----------

agg_cols = [F.count("*").alias("total_records")]

if COL_METRIC in fct_events.columns:
    agg_cols += [
        F.sum(COL_METRIC).alias("total_value"),
        F.avg(COL_METRIC).alias("avg_value"),
        F.max(COL_METRIC).alias("max_value"),
    ]

agg_by_region = (
    fct_events
    .groupBy("region", COL_STATE)
    .agg(*agg_cols)
    .orderBy("total_records", ascending=False)
)

(
    agg_by_region.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.agg_by_region")
)

print("agg_by_region gravada.")
agg_by_region.display()

# COMMAND ----------

# MAGIC %md ## 4. Sumário final — o que o BI vai consumir

# COMMAND ----------

print("=== Tabelas Gold disponíveis ===")
spark.sql(f"SHOW TABLES IN {GOLD_SCHEMA}").display()

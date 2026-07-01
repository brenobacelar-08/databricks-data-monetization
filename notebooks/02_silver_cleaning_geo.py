# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver: Limpeza, Tipagem e Enriquecimento Geográfico
# MAGIC
# MAGIC **O que este notebook faz:**
# MAGIC - Lê da Bronze (Delta), aplica regras de qualidade e limpeza
# MAGIC - Padroniza colunas geográficas (cidade, UF, lat/lon)
# MAGIC - Enriquece com coluna de Região (Norte/Sul/etc.) a partir da UF
# MAGIC - Grava em Delta Table na camada Silver
# MAGIC
# MAGIC **Ajuste a seção "Mapeamento de colunas" para bater com seu CSV real.**

# COMMAND ----------

# MAGIC %md ## Configuração

# COMMAND ----------

dbutils.widgets.text("catalog",        "data_monetization", "Catalog")
dbutils.widgets.text("bronze_table",   "events_raw",        "Tabela Bronze (origem)")
dbutils.widgets.text("silver_table",   "events",            "Tabela Silver (destino)")
# Nomes das colunas geográficas no SEU CSV — ajuste aqui:
dbutils.widgets.text("col_city",  "city",      "Coluna: cidade")
dbutils.widgets.text("col_state", "state",     "Coluna: UF/estado")
dbutils.widgets.text("col_lat",   "latitude",  "Coluna: latitude")
dbutils.widgets.text("col_lon",   "longitude", "Coluna: longitude")

CATALOG       = dbutils.widgets.get("catalog")
BRONZE_TABLE  = f"{CATALOG}.bronze.{dbutils.widgets.get('bronze_table')}"
SILVER_TABLE  = f"{CATALOG}.silver.{dbutils.widgets.get('silver_table')}"
COL_CITY      = dbutils.widgets.get("col_city")
COL_STATE     = dbutils.widgets.get("col_state")
COL_LAT       = dbutils.widgets.get("col_lat")
COL_LON       = dbutils.widgets.get("col_lon")

print(f"Origem : {BRONZE_TABLE}")
print(f"Destino: {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md ## 1. Ler Bronze

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

df = spark.table(BRONZE_TABLE)
print(f"Linhas na Bronze: {df.count()}")
df.printSchema()

# COMMAND ----------

# MAGIC %md ## 2. Limpeza geral
# MAGIC
# MAGIC Ajuste as colunas abaixo para o seu schema real.
# MAGIC Se uma coluna não existe no seu CSV, comente a linha correspondente.

# COMMAND ----------

df_clean = (
    df
    # Remove linhas completamente nulas (onde TODAS as colunas de negócio são null)
    .dropDuplicates(["_row_hash"])
    # Normaliza texto de cidade e UF: trim + uppercase
    .withColumn(COL_CITY,  F.upper(F.trim(F.col(COL_CITY))))
    .withColumn(COL_STATE, F.upper(F.trim(F.col(COL_STATE))))
    # Garante que lat/lon são Double (inferSchema às vezes os lê como String)
    .withColumn(COL_LAT, F.col(COL_LAT).cast(DoubleType()))
    .withColumn(COL_LON, F.col(COL_LON).cast(DoubleType()))
)

# COMMAND ----------

# MAGIC %md ## 3. Enriquecimento geográfico: Região por UF

# COMMAND ----------

# Mapa UF -> Região (Brasil). Adapte ou remova se seu dado não for brasileiro.
UF_TO_REGION = {
    "AC":"Norte","AP":"Norte","AM":"Norte","PA":"Norte","RO":"Norte","RR":"Norte","TO":"Norte",
    "AL":"Nordeste","BA":"Nordeste","CE":"Nordeste","MA":"Nordeste","PB":"Nordeste",
    "PE":"Nordeste","PI":"Nordeste","RN":"Nordeste","SE":"Nordeste",
    "DF":"Centro-Oeste","GO":"Centro-Oeste","MT":"Centro-Oeste","MS":"Centro-Oeste",
    "ES":"Sudeste","MG":"Sudeste","RJ":"Sudeste","SP":"Sudeste",
    "PR":"Sul","RS":"Sul","SC":"Sul",
}

mapping_expr = F.create_map([F.lit(x) for pair in UF_TO_REGION.items() for x in pair])

df_geo = (
    df_clean
    # Coluna de região derivada da UF
    .withColumn("region", mapping_expr.getItem(F.col(COL_STATE)))
    # Flag de qualidade: coordenadas válidas?
    .withColumn(
        "has_valid_coordinates",
        F.col(COL_LAT).isNotNull()
        & F.col(COL_LON).isNotNull()
        & F.col(COL_LAT).between(-90, 90)
        & F.col(COL_LON).between(-180, 180)
    )
    # Timestamp de processamento Silver
    .withColumn("_processed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md ## 4. Relatório de qualidade

# COMMAND ----------

total = df_geo.count()
invalids = df_geo.filter(~F.col("has_valid_coordinates")).count()
no_region = df_geo.filter(F.col("region").isNull()).count()

print(f"Total de linhas  : {total}")
print(f"Sem coordenadas  : {invalids} ({invalids/total*100:.1f}%)")
print(f"UF sem região    : {no_region} ({no_region/total*100:.1f}%)")

# Distribuição por região
df_geo.groupBy("region").count().orderBy("count", ascending=False).display()

# COMMAND ----------

# MAGIC %md ## 5. Gravar na Silver Delta Table

# COMMAND ----------

(
    df_geo.write
    .format("delta")
    .mode("overwrite")          # Sobrescreve a Silver com o dado mais atual
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(f"Silver gravada: {SILVER_TABLE}")
spark.sql(f"DESCRIBE DETAIL {SILVER_TABLE}").select("name","numFiles","sizeInBytes").display()

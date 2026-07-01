# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: Ingestão do CSV (Volume → Delta)
# MAGIC
# MAGIC **O que este notebook faz:**
# MAGIC - Lê CSV(s) do Volume (`/Volumes/<catalog>/bronze/raw_csv/`)
# MAGIC - Adiciona colunas de auditoria (arquivo de origem, timestamp de carga, hash da linha)
# MAGIC - Grava em Delta Table na camada Bronze (append idempotente: mesma carga não duplica)
# MAGIC
# MAGIC **Camada:** Bronze — dado cru, sem transformação, para rastreabilidade total.

# COMMAND ----------

# MAGIC %md ## Configuração (widgets — edite aqui antes de rodar)

# COMMAND ----------

dbutils.widgets.text("catalog", "data_monetization", "Catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Schema Bronze")
dbutils.widgets.text("table_name", "events_raw", "Nome da tabela Bronze")
dbutils.widgets.text("separator", ",", "Separador CSV (, ou ;)")

CATALOG       = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
TABLE_NAME    = dbutils.widgets.get("table_name")
SEPARATOR     = dbutils.widgets.get("separator")

VOLUME_PATH   = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/raw_csv"
BRONZE_TABLE  = f"{CATALOG}.{BRONZE_SCHEMA}.{TABLE_NAME}"

print(f"Volume de origem : {VOLUME_PATH}")
print(f"Tabela de destino: {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md ## 1. Descobrir arquivos disponíveis no Volume

# COMMAND ----------

import os
from pyspark.sql import functions as F

files_in_volume = dbutils.fs.ls(VOLUME_PATH)
display(files_in_volume)

# COMMAND ----------

# MAGIC %md ## 2. Ler todos os CSVs do Volume

# COMMAND ----------

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", SEPARATOR)
    .option("encoding", "UTF-8")
    .csv(VOLUME_PATH)
)

print(f"Linhas lidas: {df_raw.count()}")
print(f"Colunas detectadas: {df_raw.columns}")
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md ## 3. Adicionar colunas de auditoria (metadados de carga)

# COMMAND ----------

df_bronze = (
    df_raw
    # Arquivo CSV de origem (rastreabilidade por linha)
    .withColumn("_source_file", F.input_file_name())
    # Timestamp de quando esse batch foi carregado
    .withColumn("_ingested_at", F.current_timestamp())
    # Hash MD5 da linha completa — para dedup na Silver sem precisar de chave natural
    .withColumn(
        "_row_hash",
        F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in df_raw.columns]))
    )
)

display(df_bronze.limit(5))

# COMMAND ----------

# MAGIC %md ## 4. Gravar na Bronze Delta Table (merge/idempotência)
# MAGIC
# MAGIC Usamos `_row_hash` como chave de dedup: se você rodar este notebook duas vezes
# MAGIC com o mesmo arquivo, nenhuma linha será duplicada.

# COMMAND ----------

# Cria a tabela se não existir (primeira vez), depois faz MERGE para evitar duplicata
spark.sql(f"USE CATALOG {CATALOG}")

if not spark.catalog.tableExists(BRONZE_TABLE):
    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(BRONZE_TABLE)
    )
    print(f"Tabela {BRONZE_TABLE} criada com {df_bronze.count()} linhas.")
else:
    # Merge: insere só linhas com hash ainda não presente
    df_bronze.createOrReplaceTempView("_bronze_staging")
    spark.sql(f"""
        MERGE INTO {BRONZE_TABLE} AS target
        USING _bronze_staging AS source
        ON target._row_hash = source._row_hash
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Merge concluído em {BRONZE_TABLE}.")

# COMMAND ----------

# MAGIC %md ## 5. Validação rápida

# COMMAND ----------

total = spark.table(BRONZE_TABLE).count()
print(f"Total de linhas na Bronze agora: {total}")

spark.sql(f"DESCRIBE DETAIL {BRONZE_TABLE}").select(
    "name", "numFiles", "sizeInBytes"
).display()

# Histórico de versões Delta (time travel)
spark.sql(f"DESCRIBE HISTORY {BRONZE_TABLE} LIMIT 5").display()

# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: Ingestão do CSV de Mobilidade (Volume → Delta)
# MAGIC
# MAGIC **Schema de origem:**
# MAGIC `day, cod_municipio, nm_dist, cod_cidade_moradia, cod_distrito_moradia,`
# MAGIC `cod_cidade_trabalho, dia_semana, faixa_duracao, tipo, genero, faixa_idade,`
# MAGIC `renda, permanencia_minutos, pernoites_banda_regiao, pernoites_reg, personas, home, work, qtd`

# COMMAND ----------

dbutils.widgets.text("catalog",    "data_monetization", "Catalog")
dbutils.widgets.text("table_name", "mobilidade_raw",    "Tabela Bronze")
dbutils.widgets.text("separator",  ";",                 "Separador (ponto-e-vírgula=;  vírgula=,  TAB=\\t)")

CATALOG    = dbutils.widgets.get("catalog")
TABLE_NAME = dbutils.widgets.get("table_name")
SEPARATOR  = dbutils.widgets.get("separator")

VOLUME_PATH   = f"/Volumes/{CATALOG}/bronze/raw_csv"
BRONZE_TABLE  = f"{CATALOG}.bronze.{TABLE_NAME}"

print(f"Volume  : {VOLUME_PATH}")
print(f"Destino : {BRONZE_TABLE}")
print(f"Sep     : repr={repr(SEPARATOR)}")

# COMMAND ----------

# MAGIC %md ## 1. Listar arquivos no Volume

# COMMAND ----------

display(dbutils.fs.ls(VOLUME_PATH))

# COMMAND ----------

# MAGIC %md ## 2. Ler CSV com schema explícito
# MAGIC
# MAGIC Schema fixo para evitar erros de inferência em colunas numéricas/string mistas.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType, DateType
)

SCHEMA = StructType([
    StructField("day",                    StringType(),  True),
    StructField("cod_municipio",          StringType(),  True),
    StructField("nm_dist",                StringType(),  True),
    StructField("cod_cidade_moradia",     StringType(),  True),
    StructField("cod_distrito_moradia",   StringType(),  True),
    StructField("cod_cidade_trabalho",    StringType(),  True),
    StructField("dia_semana",             StringType(),  True),
    StructField("faixa_duracao",          StringType(),  True),
    StructField("tipo",                   StringType(),  True),
    StructField("genero",                 StringType(),  True),
    StructField("faixa_idade",            StringType(),  True),
    StructField("renda",                  StringType(),  True),
    StructField("permanencia_minutos",    DoubleType(),  True),
    StructField("pernoites_banda_regiao", StringType(),  True),
    StructField("pernoites_reg",          DoubleType(),  True),
    StructField("personas",               StringType(),  True),
    StructField("home",                   StringType(),  True),
    StructField("work",                   StringType(),  True),
    StructField("qtd",                    LongType(),    True),
])

df_raw = (
    spark.read
    .option("header", "true")
    .option("sep", SEPARATOR)
    .option("encoding", "UTF-8")
    .option("quote", '"')
    .schema(SCHEMA)
    .csv(VOLUME_PATH)
)

print(f"Linhas lidas : {df_raw.count()}")
df_raw.printSchema()
display(df_raw.limit(10))

# COMMAND ----------

# MAGIC %md ## 3. Adicionar metadados de auditoria

# COMMAND ----------

df_bronze = (
    df_raw
    # Unity Catalog exige _metadata.file_path em vez de input_file_name()
    .withColumn("_source_file",  F.col("_metadata.file_path"))
    .withColumn("_ingested_at",  F.current_timestamp())
    .withColumn("_row_hash",
        F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in df_raw.columns]))
    )
)

# COMMAND ----------

# MAGIC %md ## 4. Gravar na Bronze (idempotente via MERGE no _row_hash)

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")

if not spark.catalog.tableExists(BRONZE_TABLE):
    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(BRONZE_TABLE)
    )
    print(f"Tabela criada: {BRONZE_TABLE} — {df_bronze.count()} linhas")
else:
    df_bronze.createOrReplaceTempView("_staging")
    spark.sql(f"""
        MERGE INTO {BRONZE_TABLE} AS t
        USING _staging AS s
        ON t._row_hash = s._row_hash
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Merge concluído em {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md ## 5. Validação

# COMMAND ----------

total = spark.table(BRONZE_TABLE).count()
print(f"Total na Bronze: {total:,} linhas")

spark.sql(f"DESCRIBE HISTORY {BRONZE_TABLE} LIMIT 3").display()

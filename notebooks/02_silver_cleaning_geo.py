# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver: Limpeza, Tipagem e Enriquecimento Geográfico
# MAGIC
# MAGIC **Transformações aplicadas:**
# MAGIC - Converte `day` para DateType
# MAGIC - Limpa strings (trim, uppercase onde aplicável)
# MAGIC - Deriva `uf` e `regiao` a partir de tabela de referência IBGE por `cod_municipio`
# MAGIC - Adiciona `is_home_trip` / `is_work_trip` como boolean
# MAGIC - Flag de qualidade por linha

# COMMAND ----------

dbutils.widgets.text("catalog",      "data_monetization", "Catalog")
dbutils.widgets.text("bronze_table", "mobilidade_raw",    "Tabela Bronze")
dbutils.widgets.text("silver_table", "mobilidade",        "Tabela Silver")

CATALOG       = dbutils.widgets.get("catalog")
BRONZE_TABLE  = f"{CATALOG}.bronze.{dbutils.widgets.get('bronze_table')}"
SILVER_TABLE  = f"{CATALOG}.silver.{dbutils.widgets.get('silver_table')}"

print(f"Origem : {BRONZE_TABLE}")
print(f"Destino: {SILVER_TABLE}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DateType

# Desativa ANSI strict para que datas/valores inválidos virem null em vez de erro
spark.conf.set("spark.sql.ansi.enabled", "false")

df = spark.table(BRONZE_TABLE)
print(f"Linhas na Bronze: {df.count():,}")

# COMMAND ----------

# MAGIC %md ## 1. Limpeza e tipagem

# COMMAND ----------

df_clean = (
    df
    .dropDuplicates(["_row_hash"])
    # Data: try_to_date não quebra em valores malformados (retorna null ao invés de erro)
    .withColumn("day", F.to_date(F.col("day"), "yyyy-MM-dd"))
    # Strings: trim em todos os campos categóricos
    .withColumn("nm_dist",      F.trim(F.col("nm_dist")))
    .withColumn("tipo",         F.trim(F.upper(F.col("tipo"))))
    .withColumn("genero",       F.trim(F.upper(F.col("genero"))))
    .withColumn("faixa_idade",  F.trim(F.col("faixa_idade")))
    .withColumn("renda",        F.trim(F.col("renda")))
    .withColumn("dia_semana",   F.trim(F.col("dia_semana")))
    .withColumn("faixa_duracao",F.trim(F.col("faixa_duracao")))
    .withColumn("personas",     F.trim(F.col("personas")))
    # Booleanos derivados
    .withColumn("is_home_trip", F.col("home").isin("1","true","True","S","s","sim"))
    .withColumn("is_work_trip", F.col("work").isin("1","true","True","S","s","sim"))
    # Flag de qualidade: qtd não pode ser nulo ou negativo
    .withColumn("is_valid", F.col("qtd").isNotNull() & (F.col("qtd") >= 0))
)

# COMMAND ----------

# MAGIC %md ## 2. Enriquecimento Geográfico: UF e Região por cod_municipio (IBGE)
# MAGIC
# MAGIC Os dois primeiros dígitos do código IBGE de município identificam o estado.
# MAGIC Isso dispensa join com tabela externa — derivamos UF diretamente do código.

# COMMAND ----------

# Mapa dos primeiros 2 dígitos IBGE → UF
IBGE_PREFIX_TO_UF = {
    "11":"RO","12":"AC","13":"AM","14":"RR","15":"PA","16":"AP","17":"TO",
    "21":"MA","22":"PI","23":"CE","24":"RN","25":"PB","26":"PE","27":"AL",
    "28":"SE","29":"BA",
    "31":"MG","32":"ES","33":"RJ","35":"SP",
    "41":"PR","42":"SC","43":"RS",
    "50":"MS","51":"MT","52":"GO","53":"DF",
}

UF_TO_REGION = {
    "RO":"Norte","AC":"Norte","AM":"Norte","RR":"Norte","PA":"Norte","AP":"Norte","TO":"Norte",
    "MA":"Nordeste","PI":"Nordeste","CE":"Nordeste","RN":"Nordeste","PB":"Nordeste",
    "PE":"Nordeste","AL":"Nordeste","SE":"Nordeste","BA":"Nordeste",
    "MG":"Sudeste","ES":"Sudeste","RJ":"Sudeste","SP":"Sudeste",
    "PR":"Sul","SC":"Sul","RS":"Sul",
    "MS":"Centro-Oeste","MT":"Centro-Oeste","GO":"Centro-Oeste","DF":"Centro-Oeste",
}

ibge_map  = F.create_map([F.lit(x) for pair in IBGE_PREFIX_TO_UF.items() for x in pair])
region_map = F.create_map([F.lit(x) for pair in UF_TO_REGION.items() for x in pair])

df_geo = (
    df_clean
    .withColumn("ibge_prefix", F.substring(F.col("cod_municipio"), 1, 2))
    .withColumn("uf",    ibge_map.getItem(F.col("ibge_prefix")))
    .withColumn("regiao", region_map.getItem(F.col("uf")))
    .drop("ibge_prefix")
    .withColumn("_processed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md ## 3. Relatório de qualidade

# COMMAND ----------

total    = df_geo.count()
invalids = df_geo.filter(~F.col("is_valid")).count()
sem_uf   = df_geo.filter(F.col("uf").isNull()).count()
sem_data = df_geo.filter(F.col("day").isNull()).count()

print(f"Total de linhas   : {total:,}")
print(f"qtd inválida      : {invalids:,}  ({invalids/total*100:.1f}%)")
print(f"Sem UF detectada  : {sem_uf:,}   ({sem_uf/total*100:.1f}%)")
print(f"Sem data          : {sem_data:,}  ({sem_data/total*100:.1f}%)")

print("\n— Distribuição por Região —")
df_geo.groupBy("regiao").agg(
    F.count("*").alias("registros"),
    F.sum("qtd").alias("total_qtd")
).orderBy("total_qtd", ascending=False).display()

# COMMAND ----------

# MAGIC %md ## 4. Gravar Silver

# COMMAND ----------

(
    df_geo.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(f"Silver gravada: {SILVER_TABLE}  ({df_geo.count():,} linhas)")

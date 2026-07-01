# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: Modelo Dimensional de Mobilidade
# MAGIC
# MAGIC Tabelas geradas:
# MAGIC - `dim_municipio`   — dimensão geográfica (cod_municipio, nm_dist, uf, regiao)
# MAGIC - `dim_perfil`      — dimensão demográfica (genero, faixa_idade, renda, personas)
# MAGIC - `fct_mobilidade`  — tabela fato (qtd, permanencia_minutos por dia/município/perfil)
# MAGIC - `agg_municipio`   — KPI por município (para mapas e ranking)
# MAGIC - `agg_perfil`      — KPI por perfil demográfico (para segmentação)
# MAGIC - `agg_temporal`    — série temporal diária (para gráfico de linha)
# MAGIC - `agg_fluxo`       — fluxo moradia→trabalho (para OD - origem/destino)

# COMMAND ----------

dbutils.widgets.text("catalog",      "data_monetization", "Catalog")
dbutils.widgets.text("silver_table", "mobilidade",        "Tabela Silver")

CATALOG      = dbutils.widgets.get("catalog")
SILVER_TABLE = f"{CATALOG}.silver.{dbutils.widgets.get('silver_table')}"
GOLD         = f"{CATALOG}.gold"

print(f"Origem : {SILVER_TABLE}")
print(f"Destino: {GOLD}.*")

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table(SILVER_TABLE).filter(F.col("is_valid"))
print(f"Linhas válidas na Silver: {df.count():,}")

# COMMAND ----------

# MAGIC %md ## 1. dim_municipio

# COMMAND ----------

dim_municipio = (
    df.select("cod_municipio", "nm_dist", "uf", "regiao")
    .distinct()
    .withColumn("municipio_sk", F.monotonically_increasing_id())
    .orderBy("uf", "nm_dist")
)

dim_municipio.write.format("delta").mode("overwrite").option("overwriteSchema","true") \
    .saveAsTable(f"{GOLD}.dim_municipio")
print(f"dim_municipio: {dim_municipio.count():,} municípios")
dim_municipio.display()

# COMMAND ----------

# MAGIC %md ## 2. dim_perfil

# COMMAND ----------

dim_perfil = (
    df.select("genero", "faixa_idade", "renda", "personas")
    .distinct()
    .withColumn("perfil_sk", F.monotonically_increasing_id())
    .orderBy("genero", "faixa_idade", "renda")
)

dim_perfil.write.format("delta").mode("overwrite").option("overwriteSchema","true") \
    .saveAsTable(f"{GOLD}.dim_perfil")
print(f"dim_perfil: {dim_perfil.count():,} combinações")
dim_perfil.display()

# COMMAND ----------

# MAGIC %md ## 3. fct_mobilidade (tabela fato)

# COMMAND ----------

fct_mobilidade = (
    df
    .join(dim_municipio.select("municipio_sk","cod_municipio","nm_dist"),
          on=["cod_municipio","nm_dist"], how="left")
    .join(dim_perfil.select("perfil_sk","genero","faixa_idade","renda","personas"),
          on=["genero","faixa_idade","renda","personas"], how="left")
    .select(
        "day",
        F.year("day").alias("ano"),
        F.month("day").alias("mes"),
        F.dayofmonth("day").alias("dia"),
        "dia_semana",
        "municipio_sk", "cod_municipio", "nm_dist", "uf", "regiao",
        "perfil_sk", "genero", "faixa_idade", "renda", "personas",
        "tipo", "faixa_duracao",
        "cod_cidade_moradia", "cod_distrito_moradia", "cod_cidade_trabalho",
        "is_home_trip", "is_work_trip",
        "qtd",
        "permanencia_minutos",
        "pernoites_banda_regiao", "pernoites_reg",
        "is_valid",
    )
)

fct_mobilidade.write.format("delta").mode("overwrite").option("overwriteSchema","true") \
    .saveAsTable(f"{GOLD}.fct_mobilidade")
print(f"fct_mobilidade: {fct_mobilidade.count():,} linhas")

# COMMAND ----------

# MAGIC %md ## 4. agg_municipio — KPI por município (ranking e mapa)

# COMMAND ----------

agg_municipio = (
    fct_mobilidade.groupBy("cod_municipio","nm_dist","uf","regiao")
    .agg(
        F.sum("qtd").alias("total_visitas"),
        F.avg("permanencia_minutos").alias("media_permanencia_min"),
        F.countDistinct("day").alias("dias_com_dados"),
        F.sum(F.when(F.col("is_home_trip"), F.col("qtd")).otherwise(0)).alias("visitas_moradia"),
        F.sum(F.when(F.col("is_work_trip"), F.col("qtd")).otherwise(0)).alias("visitas_trabalho"),
    )
    .orderBy("total_visitas", ascending=False)
)

agg_municipio.write.format("delta").mode("overwrite").option("overwriteSchema","true") \
    .saveAsTable(f"{GOLD}.agg_municipio")
print("agg_municipio gravada.")
agg_municipio.display()

# COMMAND ----------

# MAGIC %md ## 5. agg_perfil — segmentação demográfica

# COMMAND ----------

agg_perfil = (
    fct_mobilidade.groupBy("genero","faixa_idade","renda","personas")
    .agg(
        F.sum("qtd").alias("total_visitas"),
        F.avg("permanencia_minutos").alias("media_permanencia_min"),
        F.countDistinct("nm_dist").alias("municipios_visitados"),
    )
    .orderBy("total_visitas", ascending=False)
)

agg_perfil.write.format("delta").mode("overwrite").option("overwriteSchema","true") \
    .saveAsTable(f"{GOLD}.agg_perfil")
print("agg_perfil gravada.")
agg_perfil.display()

# COMMAND ----------

# MAGIC %md ## 6. agg_temporal — série temporal diária

# COMMAND ----------

agg_temporal = (
    fct_mobilidade.groupBy("day","ano","mes","dia_semana")
    .agg(
        F.sum("qtd").alias("total_visitas"),
        F.avg("permanencia_minutos").alias("media_permanencia_min"),
        F.countDistinct("cod_municipio").alias("municipios_ativos"),
    )
    .orderBy("day")
)

agg_temporal.write.format("delta").mode("overwrite").option("overwriteSchema","true") \
    .saveAsTable(f"{GOLD}.agg_temporal")
print("agg_temporal gravada.")
agg_temporal.display()

# COMMAND ----------

# MAGIC %md ## 7. agg_fluxo — origem (moradia) → destino (trabalho)
# MAGIC
# MAGIC Mostra de qual município as pessoas saem para trabalhar em cada município.
# MAGIC Base para análise OD (Origem-Destino), muito valorizada em estudos de mobilidade.

# COMMAND ----------

agg_fluxo = (
    fct_mobilidade
    .filter(F.col("is_work_trip") & F.col("cod_cidade_moradia").isNotNull())
    .groupBy("cod_cidade_moradia","cod_municipio","nm_dist","uf","regiao")
    .agg(F.sum("qtd").alias("fluxo_qtd"))
    .orderBy("fluxo_qtd", ascending=False)
)

agg_fluxo.write.format("delta").mode("overwrite").option("overwriteSchema","true") \
    .saveAsTable(f"{GOLD}.agg_fluxo")
print("agg_fluxo gravada.")
agg_fluxo.display()

# COMMAND ----------

# MAGIC %md ## Sumário Gold

# COMMAND ----------

spark.sql(f"SHOW TABLES IN {GOLD}").display()

# Databricks notebook source
# MAGIC %md
# MAGIC # Export Gold → BigQuery
# MAGIC
# MAGIC Exporta as tabelas Gold do Unity Catalog para um dataset BigQuery.
# MAGIC Pré-requisito: biblioteca `spark-bigquery-with-dependencies` instalada no cluster.
# MAGIC Veja `bigquery/README.md` para instruções completas.

# COMMAND ----------

dbutils.widgets.text("catalog",          "data_monetization", "Catalog Databricks")
dbutils.widgets.text("gcp_project",      "",                  "Projeto GCP")
dbutils.widgets.text("bq_dataset",       "data_monetization", "Dataset BigQuery")
dbutils.widgets.text("gcs_temp_bucket",  "",                  "Bucket GCS (temp)")
dbutils.widgets.text("credentials_path", "", "Path da chave JSON no Volume")

CATALOG          = dbutils.widgets.get("catalog")
GCP_PROJECT      = dbutils.widgets.get("gcp_project")
BQ_DATASET       = dbutils.widgets.get("bq_dataset")
GCS_TEMP_BUCKET  = dbutils.widgets.get("gcs_temp_bucket")
CREDENTIALS_PATH = dbutils.widgets.get("credentials_path")

if not GCP_PROJECT or not GCS_TEMP_BUCKET:
    raise ValueError("Preencha gcp_project e gcs_temp_bucket nos widgets antes de rodar.")

print(f"Destino: {GCP_PROJECT}.{BQ_DATASET}")

# COMMAND ----------

# MAGIC %md ## Função de export

# COMMAND ----------

def export_to_bq(spark_table: str, bq_table: str) -> None:
    """Lê uma tabela Delta do Unity Catalog e grava no BigQuery via conector Spark."""
    df = spark.table(spark_table)

    write_opts = {
        "table":          f"{GCP_PROJECT}:{BQ_DATASET}.{bq_table}",
        "temporaryGcsBucket": GCS_TEMP_BUCKET,
        "writeMethod":    "indirect",   # usa GCS como staging — obrigatório para escrita
        "createDisposition": "CREATE_IF_NEEDED",
    }
    if CREDENTIALS_PATH:
        write_opts["credentialsFile"] = CREDENTIALS_PATH

    df.write.format("bigquery").options(**write_opts).mode("overwrite").save()
    print(f"  ✓ {spark_table} → {GCP_PROJECT}:{BQ_DATASET}.{bq_table}")

# COMMAND ----------

# MAGIC %md ## Exportar todas as tabelas Gold

# COMMAND ----------

gold_tables = {
    f"{CATALOG}.gold.dim_geography": "dim_geography",
    f"{CATALOG}.gold.fct_events":    "fct_events",
    f"{CATALOG}.gold.agg_by_region": "agg_by_region",
}

for src, dest in gold_tables.items():
    print(f"Exportando {src}...")
    export_to_bq(src, dest)

print("\nExport concluído. Verifique as tabelas no BigQuery Console.")

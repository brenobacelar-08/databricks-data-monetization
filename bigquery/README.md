# Export para BigQuery

Esta pasta contém tudo necessário para levar a camada **Gold** do Databricks para o
BigQuery — sem reescrever nenhum notebook de engenharia.

## Quando ativar isso?

Quando quiser:
- Compartilhar dados com times que vivem no ecossistema GCP/Looker Studio.
- Usar BigQuery ML ou ferramentas GCP nativas.
- Separar o custo de BI do custo do Databricks.

## Pré-requisitos

1. **Conta GCP** com BigQuery habilitado (free tier inicial: 10 GB/mês de armazenamento +
   1 TB/mês de queries — suficiente para aprender).
2. **Service account** no GCP com roles `BigQuery Data Editor` e `BigQuery Job User`.
   Gere a chave JSON e anote o caminho.
3. **Conector Spark-BigQuery** instalado no cluster Databricks (configure em
   `Compute > Libraries > Maven`:
   `com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1`).

## Como exportar

1. Abra `export_to_bigquery.py` no Databricks como notebook.
2. Configure os widgets:
   - `gcp_project`: seu projeto GCP (ex: `meu-projeto-123`)
   - `bq_dataset`: dataset BigQuery de destino (ex: `data_monetization`)
   - `gcs_temp_bucket`: bucket GCS temporário (o conector precisa disso para staging)
   - `credentials_path`: caminho da chave JSON do service account **dentro do Volume**
     (`/Volumes/data_monetization/bronze/raw_csv/credentials.json`) — nunca commit
     essa chave no Git.
3. Rode o notebook — ele exporta `dim_geography`, `fct_events` e `agg_by_region`.

## Mapeamento de tipos

Veja [schema_mapping.md](schema_mapping.md) para a tabela de equivalência entre tipos
Delta Lake e BigQuery. Nenhuma conversão manual necessária — o conector cuida disso,
mas é bom entender para quando BigQuery reclamar de algo.

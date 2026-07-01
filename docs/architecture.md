# Arquitetura

## 1. Por que Unity Catalog Volumes (e não DBFS/FileStore)

Volumes são a forma atual e recomendada da Databricks para guardar arquivos não-tabulares
(CSV, imagens, PDFs) com governança (permissões, linhagem) dentro do Unity Catalog. DBFS
root e `/FileStore` estão em depreciação para esse uso. Por isso este projeto usa:

```
/Volumes/<catalog>/<schema>/raw_csv/
```

**Nota sobre Community Edition:** a Databricks "Community Edition" clássica (gratuita,
antiga) **não tem Unity Catalog**, e portanto não tem Volumes — só DBFS. Em 2024 a
Databricks lançou a **Free Edition**, que substitui a Community Edition e já vem com
Unity Catalog habilitado, incluindo Volumes, Delta Lake completo e Databricks SQL. Se
você criou sua conta recentemente, provavelmente já está na Free Edition — confirme indo
em **Catalog** no menu lateral: se existir "Unity Catalog" com catalogs/schemas/volumes,
você está coberto. Se só existir DBFS, você está na Community Edition antiga e vai
precisar criar uma conta nova na Free Edition para seguir este projeto como desenhado.

## 2. Arquitetura Medallion

| Camada  | Formato | Responsabilidade | Quem consome |
|---------|---------|-------------------|---------------|
| Bronze  | Delta   | Cópia fiel do CSV + metadados de ingestão (arquivo de origem, timestamp de carga, linha) | Engenharia (auditoria/reprocessamento) |
| Silver  | Delta   | Dados limpos, tipados, deduplicados, enriquecidos geograficamente (UF, região, lat/lon padronizados) | Engenharia/Analytics Engineering |
| Gold    | Delta   | Modelo dimensional (fatos + dimensões), métricas de negócio agregadas | BI, dashboards, exportação externa |

Princípio: cada camada só lê da camada anterior. Nunca se escreve direto em Gold a partir
do CSV — isso garante que você sempre pode reprocessar do zero (Bronze é a fonte da
verdade imutável).

## 3. Enriquecimento Geográfico

A camada Silver é onde colunas geográficas do seu CSV (ex: cidade, UF, CEP, lat/lon) são
padronizadas:

- Normalização de texto (acentos, caixa, espaços) em nomes de cidade/UF.
- Validação de lat/lon (faixa válida, não nulos quando esperado).
- Bucket geográfico (ex: Região Norte/Sul/Sudeste/Centro-Oeste/Nordeste a partir da UF).
- Espaço reservado para H3/geohash se você quiser granularidade de hexágono mais adiante
  (útil se algum dia este produto se conectar aos seus projetos de geo, como
  `geocity`/`smart-steps-geo`).

Essa lógica vive em [src/geo_utils.py](../src/geo_utils.py) para ser reutilizável tanto em
notebook Databricks quanto, futuramente, em um job de export para BigQuery.

## 4. Exposição local vs. preparação para BigQuery

**Agora (local):** Gold fica em Delta Tables dentro do Unity Catalog do seu workspace
Databricks. BI roda via Databricks SQL Dashboards/Genie, consumindo essas tabelas
diretamente — sem custo de saída de dados, sem infraestrutura extra.

**Depois (BigQuery, quando você quiser):** a pasta [bigquery/](../bigquery/) já contém:
- Mapeamento de tipos Delta → BigQuery (`schema_mapping.md`).
- Script de export (`export_to_bigquery.py`) que lê as tabelas Gold via Spark e escreve via
  o conector `spark-bigquery-connector`, sem precisar mudar nada nas camadas Bronze/Silver.

A decisão de desenho é: **Gold é o único contrato externo**. Qualquer destino novo
(BigQuery, Snowflake, uma API) só precisa saber ler da Gold — Bronze/Silver continuam
sendo detalhe de implementação interno do Databricks.

## 5. Diagrama de dependências dos notebooks

```
00_create_catalog_schema_volume.sql   (1x, manual, no SQL Editor)
        │
        ▼
01_bronze_ingestion.py                (roda a cada novo arquivo no Volume)
        │
        ▼
02_silver_cleaning_geo.py
        │
        ▼
03_gold_aggregations.py
        │
        ├──► 04_bi_queries.sql        (Databricks SQL / dashboard)
        │
        └──► bigquery/export_to_bigquery.py   (quando ativar BigQuery)
```

# 📦 Databricks Data Monetization Lab

Produto de dados pessoal para aprender (e provar na prática) engenharia de dados, BI e
governança no Databricks — usando Unity Catalog Volumes, Delta Lake e arquitetura
medallion (Bronze → Silver → Gold), com dados geográficos e caminho pronto para
exportar para o BigQuery.

> Objetivo real: não é só um curso. É um projeto que você pode mostrar para gestor/sênior
> como produto de dados ponta a ponta — ingestão, qualidade, modelagem, BI e potencial
> de monetização (relatórios, APIs de dados, dashboards para clientes internos/externos).

## Por que este projeto existe

Você entra com os dados (CSV) → o pipeline organiza, limpa, enriquece geograficamente e
expõe em camadas analíticas → BI consome a camada Gold → depois, sem reescrever nada,
os mesmos dados Gold viram a base para exportação ao BigQuery (multi-cloud / distribuição
para fora do Databricks).

## Arquitetura (visão rápida)

```
Local CSV
   │  (upload manual, depois automatizável)
   ▼
Unity Catalog Volume  (/Volumes/<catalog>/<schema>/raw_csv)
   │
   ▼
BRONZE  (Delta, dados crus + metadados de carga)
   │  limpeza, tipagem, dedup, enriquecimento geo (UF/região/lat-lon)
   ▼
SILVER  (Delta, dados validados e conformados)
   │  agregações de negócio, métricas, dimensões geográficas
   ▼
GOLD    (Delta, modelo estrela — pronto para BI)
   │
   ├──► Databricks SQL Dashboards / Genie (BI local)
   │
   └──► Export job ──► BigQuery (preparado, ativável quando quiser ir multi-cloud)
```

Detalhe completo em [docs/architecture.md](docs/architecture.md).

## Estrutura do repositório

```
databricks-data-monetization/
├── setup/                  # SQL de criação de catalog/schema/volume (Unity Catalog)
├── notebooks/              # Notebooks Databricks: engenharia (bronze/silver/gold) + BI
├── src/                    # Código Python reutilizável (config, geo utils)
├── bigquery/               # Preparação para exportar a camada Gold ao BigQuery
├── config/                 # Configuração de ambiente (catalog, schema, paths)
└── docs/                   # Arquitetura, plano de aprendizado, tese de monetização
```

## Como começar (Databricks Free/Community Edition)

1. Crie uma conta em [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
   (a versão atual já suporta Unity Catalog e Volumes — a antiga Community Edition clássica
   não suporta Volumes; veja a nota em [docs/architecture.md](docs/architecture.md)).
2. Rode `setup/00_create_catalog_schema_volume.sql` no SQL Editor do Databricks para criar
   o catalog, schema e volume.
3. Faça upload manual do(s) seu(s) CSV em
   `/Volumes/<catalog>/<schema>/raw_csv/` (pelo Catalog Explorer, botão "Upload").
4. Importe os notebooks da pasta `notebooks/` no seu workspace (Workspace → Import → File),
   ajuste o widget `catalog`/`schema` no topo de cada notebook e rode em ordem:
   `01_bronze_ingestion` → `02_silver_cleaning_geo` → `03_gold_aggregations`.
5. Use `notebooks/04_bi_queries.sql` no Databricks SQL para montar seu primeiro dashboard.
6. Quando quiser ir para BigQuery, siga [bigquery/README.md](bigquery/README.md).

## Plano de aprendizado

Sequência sugerida de estudo enquanto constrói este produto está em
[docs/learning_plan.md](docs/learning_plan.md).

## Tese de monetização

Como esse pipeline pode virar produto de dados vendável (interno ou externo) está em
[docs/monetization.md](docs/monetization.md).

## Status

🚧 Em construção — schema do CSV real ainda será definido. Os notebooks foram escritos de
forma genérica/configurável (via widgets) para se adaptar ao seu CSV assim que ele for
carregado no Volume. Veja `config/settings.example.yml` para o contrato de schema esperado.

# Plano de Aprendizado

Sequência sugerida — cada etapa usa o que você já construiu na anterior, então você
aprende fazendo, não só lendo.

## Semana 1 — Fundamentos + Engenharia

- [ ] Criar conta Databricks Free Edition, entender a diferença entre Workspace,
      Catalog, Schema e Volume (Unity Catalog).
- [ ] Rodar `setup/00_create_catalog_schema_volume.sql`.
- [ ] Subir seu CSV real no Volume e rodar `01_bronze_ingestion.py`.
- [ ] Entender: por que ler com `spark.read.csv` em vez de `pandas.read_csv`? (resposta:
      distribuição, escala, integração nativa com Delta/Unity Catalog — mesmo em volume
      pequeno, o objetivo é o hábito do padrão certo).
- [ ] Conceito-chave: **schema evolution** e **idempotência** de ingestão (o que acontece
      se você rodar o notebook duas vezes com o mesmo arquivo?).

## Semana 2 — Qualidade e Modelagem

- [ ] Rodar `02_silver_cleaning_geo.py`, entender cada regra de limpeza aplicada.
- [ ] Estudar arquitetura medallion (Bronze/Silver/Gold) — por que ela existe, quais
      problemas ela evita (reprocessamento caro, dado sujo direto em produção).
- [ ] Rodar `03_gold_aggregations.py`, entender modelagem dimensional (fato vs. dimensão).
- [ ] Conceito-chave: **Delta Lake time travel** — rode `DESCRIBE HISTORY` em uma tabela
      Gold e veja as versões.

## Semana 3 — BI e Governança

- [ ] Criar seu primeiro Databricks SQL Dashboard a partir de `04_bi_queries.sql`.
- [ ] Configurar permissões de Unity Catalog (`GRANT SELECT`) — simule um cenário onde
      "um cliente externo" só pode ler Gold, nunca Bronze/Silver.
- [ ] Estudar **lineage** no Catalog Explorer (Databricks rastreia automaticamente de onde
      cada tabela Gold veio).
- [ ] Opcional: experimentar Genie (BI conversacional) apontando para a camada Gold.

## Semana 4 — Multi-cloud / BigQuery

- [ ] Ler `bigquery/schema_mapping.md`, entender diferenças de tipos Delta ↔ BigQuery.
- [ ] Criar um projeto GCP free tier, criar um dataset BigQuery vazio.
- [ ] Configurar credenciais e rodar `bigquery/export_to_bigquery.py` (mesmo que ainda sem
      dados reais, valide a conexão).
- [ ] Conceito-chave: por que exportar só a Gold e não Bronze/Silver para fora do
      Databricks (custo, governança, contrato de dados estável).

## Depois disso

- Automatizar ingestão (Databricks Jobs/Workflows acionado por chegada de arquivo no Volume).
- Testes de qualidade de dados (Delta Live Tables expectations ou `dbt` + Databricks).
- Versionamento dos notebooks como Databricks Asset Bundles (infra como código).

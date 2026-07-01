-- ============================================================================
-- Setup inicial do Unity Catalog para o produto de dados.
-- Rode este script no SQL Editor do Databricks (não em um notebook Python).
-- Ajuste os nomes abaixo se quiser outro padrão de nomenclatura.
-- ============================================================================

-- 1. Catalog dedicado ao produto de dados.
CREATE CATALOG IF NOT EXISTS data_monetization
  COMMENT 'Produto de dados pessoal: ingestao, BI e export para BigQuery';

USE CATALOG data_monetization;

-- 2. Schemas por camada da arquitetura medallion.
CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Dados crus, copia fiel do CSV de origem';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Dados limpos, tipados e enriquecidos geograficamente';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Modelo dimensional pronto para BI e export externo';

-- 3. Volume para receber os arquivos CSV de origem (upload manual via Catalog Explorer).
CREATE VOLUME IF NOT EXISTS bronze.raw_csv
  COMMENT 'Arquivos CSV de origem, enviados manualmente ou via job de ingestao';

-- Caminho resultante para upload do seu CSV (use no Catalog Explorer, botao "Upload to volume"):
--   /Volumes/data_monetization/bronze/raw_csv/

-- 4. Conferir o que foi criado.
SHOW SCHEMAS IN data_monetization;
SHOW VOLUMES IN data_monetization.bronze;

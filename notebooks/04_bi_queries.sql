-- ============================================================================
-- 04 — BI: Queries para Dashboards (Databricks SQL)
-- Rode estas queries no Databricks SQL Editor e salve como visualizações
-- para montar seu Dashboard (botão "Create Dashboard" no canto superior direito).
--
-- Ajuste o catalog/schema se você usou nomes diferentes no setup.
-- ============================================================================

USE CATALOG data_monetization;
USE SCHEMA gold;

-- ============================================================================
-- KPI 1: Total de registros no dataset (card de resumo)
-- ============================================================================
SELECT
    COUNT(*)                                    AS total_records,
    COUNT(DISTINCT state)                       AS estados_cobertos,
    COUNT(DISTINCT city)                        AS cidades_cobertas,
    COUNT(DISTINCT region)                      AS regioes_cobertas,
    SUM(CASE WHEN has_valid_coordinates THEN 1 ELSE 0 END) AS com_coordenadas
FROM fct_events;

-- ============================================================================
-- KPI 2: Distribuição por Região (gráfico de barras ou pizza)
-- ============================================================================
SELECT
    region,
    total_records,
    ROUND(total_records * 100.0 / SUM(total_records) OVER (), 1) AS pct_total
FROM agg_by_region
WHERE region IS NOT NULL
ORDER BY total_records DESC;

-- ============================================================================
-- KPI 3: Top 10 estados por volume de registros
-- ============================================================================
SELECT
    state,
    region,
    total_records
FROM agg_by_region
WHERE state IS NOT NULL
ORDER BY total_records DESC
LIMIT 10;

-- ============================================================================
-- KPI 4: Top 20 cidades por volume de registros
-- (Use para mapa de bolhas no Databricks ou Tableau/Power BI apontando para cá)
-- ============================================================================
SELECT
    city,
    state,
    region,
    COUNT(*) AS total_records
FROM fct_events
GROUP BY city, state, region
ORDER BY total_records DESC
LIMIT 20;

-- ============================================================================
-- KPI 5: Série temporal — evolução mensal de registros
-- (Só funciona se seu CSV tiver coluna de data)
-- ============================================================================
SELECT
    event_year,
    event_month,
    MAKE_DATE(event_year, event_month, 1)  AS mes,
    COUNT(*)                               AS total_records
FROM fct_events
WHERE event_year IS NOT NULL
GROUP BY event_year, event_month
ORDER BY event_year, event_month;

-- ============================================================================
-- KPI 6: Qualidade dos dados — % de linhas com coordenadas válidas por estado
-- (Bom para slide de "governança de dados" para o gestor)
-- ============================================================================
SELECT
    state,
    COUNT(*)                                                       AS total,
    SUM(CASE WHEN has_valid_coordinates THEN 1 ELSE 0 END)        AS com_coord,
    ROUND(
        SUM(CASE WHEN has_valid_coordinates THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*), 1
    )                                                              AS pct_com_coord
FROM fct_events
WHERE state IS NOT NULL
GROUP BY state
ORDER BY total DESC;

-- ============================================================================
-- KPI 7: Dimensão geográfica completa — útil para filtros de dashboard
-- ============================================================================
SELECT
    geo_sk,
    city,
    state,
    region
FROM dim_geography
ORDER BY region, state, city;

-- ============================================================================
-- COMO MONTAR O DASHBOARD NO DATABRICKS SQL:
-- 1. No SQL Editor, rode cada query acima.
-- 2. Abaixo do resultado, clique em "+" > "Add visualization" para criar
--    bar chart, pie, line ou mapa.
-- 3. Clique em "Save to dashboard" no topo.
-- 4. Nomeie o dashboard (ex: "Produto de Dados — Visão Executiva") e adicione
--    todos os painéis que quiser.
-- 5. Compartilhe o link do dashboard com seu gestor/sênior.
-- ============================================================================

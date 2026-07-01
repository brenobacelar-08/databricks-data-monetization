-- ============================================================================
-- 04 — BI: Queries de Mobilidade para Dashboard (Databricks SQL)
--
-- Como usar:
-- 1. Abra cada bloco no Databricks SQL Editor
-- 2. Clique em "Add visualization" abaixo do resultado
-- 3. Salve no Dashboard "Mobilidade — Visão Executiva"
-- ============================================================================

USE CATALOG data_monetization;
USE SCHEMA gold;

-- ============================================================================
-- KPI CARD 1: Resumo geral do dataset
-- Tipo de visualização: Counter (cartões de número)
-- ============================================================================
SELECT
    SUM(total_visitas)                            AS total_visitas,
    COUNT(DISTINCT cod_municipio)                  AS municipios_cobertos,
    COUNT(DISTINCT uf)                             AS estados_cobertos,
    ROUND(AVG(media_permanencia_min), 1)           AS media_permanencia_min
FROM agg_municipio;

-- ============================================================================
-- KPI 2: Visitas por Região
-- Tipo: Bar chart horizontal (região no eixo Y, total no X)
-- ============================================================================
SELECT
    regiao,
    SUM(total_visitas)                                          AS total_visitas,
    ROUND(SUM(total_visitas) * 100.0 / SUM(SUM(total_visitas)) OVER (), 1) AS pct
FROM agg_municipio
WHERE regiao IS NOT NULL
GROUP BY regiao
ORDER BY total_visitas DESC;

-- ============================================================================
-- KPI 3: Top 15 municípios por volume de visitas
-- Tipo: Bar chart vertical ou tabela com destaque
-- ============================================================================
SELECT
    nm_dist                                   AS municipio,
    uf,
    regiao,
    total_visitas,
    ROUND(media_permanencia_min, 1)            AS permanencia_media_min,
    visitas_moradia,
    visitas_trabalho
FROM agg_municipio
ORDER BY total_visitas DESC
LIMIT 15;

-- ============================================================================
-- KPI 4: Série temporal — visitas por dia
-- Tipo: Line chart (day no X, total_visitas no Y)
-- ============================================================================
SELECT
    day,
    dia_semana,
    total_visitas,
    municipios_ativos,
    ROUND(media_permanencia_min, 1) AS permanencia_media_min
FROM agg_temporal
ORDER BY day;

-- ============================================================================
-- KPI 5: Perfil demográfico — visitas por gênero e faixa de idade
-- Tipo: Heatmap ou grouped bar (genero + faixa_idade)
-- ============================================================================
SELECT
    genero,
    faixa_idade,
    SUM(total_visitas)               AS total_visitas,
    ROUND(AVG(media_permanencia_min), 1) AS permanencia_media_min
FROM agg_perfil
WHERE genero IS NOT NULL AND faixa_idade IS NOT NULL
GROUP BY genero, faixa_idade
ORDER BY genero, total_visitas DESC;

-- ============================================================================
-- KPI 6: Distribuição por faixa de renda
-- Tipo: Pie chart ou bar
-- ============================================================================
SELECT
    renda,
    SUM(total_visitas)                                              AS total_visitas,
    ROUND(SUM(total_visitas) * 100.0 / SUM(SUM(total_visitas)) OVER (), 1) AS pct
FROM agg_perfil
WHERE renda IS NOT NULL
GROUP BY renda
ORDER BY total_visitas DESC;

-- ============================================================================
-- KPI 7: Distribuição por personas
-- Tipo: Bar ou tabela — mostra qual persona tem mais presença
-- ============================================================================
SELECT
    personas,
    SUM(total_visitas)               AS total_visitas,
    SUM(municipios_visitados)        AS municipios_distintos
FROM agg_perfil
WHERE personas IS NOT NULL
GROUP BY personas
ORDER BY total_visitas DESC;

-- ============================================================================
-- KPI 8: Padrão por dia da semana
-- Tipo: Bar chart (dia_semana no X) — mostra quais dias têm mais mobilidade
-- ============================================================================
SELECT
    dia_semana,
    SUM(total_visitas)               AS total_visitas,
    ROUND(AVG(media_permanencia_min), 1) AS permanencia_media_min
FROM agg_temporal
WHERE dia_semana IS NOT NULL
GROUP BY dia_semana
ORDER BY total_visitas DESC;

-- ============================================================================
-- KPI 9: Fluxo OD — top 10 rotas moradia → trabalho
-- Tipo: Tabela com pares de cidades
-- (base para vendas de produto de mobilidade para prefeituras/consultorias)
-- ============================================================================
SELECT
    f.cod_cidade_moradia                    AS origem_cod,
    f.cod_municipio                         AS destino_cod,
    m.nm_dist                               AS destino_nome,
    m.uf                                    AS destino_uf,
    SUM(f.fluxo_qtd)                        AS fluxo_total
FROM agg_fluxo f
JOIN dim_municipio m ON f.cod_municipio = m.cod_municipio
GROUP BY f.cod_cidade_moradia, f.cod_municipio, m.nm_dist, m.uf
ORDER BY fluxo_total DESC
LIMIT 10;

-- ============================================================================
-- KPI 10: Qualidade dos dados por UF (slide de governança)
-- Tipo: Tabela — mostra ao gestor que você monitora a qualidade
-- ============================================================================
SELECT
    uf,
    regiao,
    SUM(total_visitas)  AS total_visitas,
    dias_com_dados
FROM agg_municipio
WHERE uf IS NOT NULL
GROUP BY uf, regiao, dias_com_dados
ORDER BY total_visitas DESC;

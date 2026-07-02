-- ============================================================================
-- 04 — BI: KPIs de Mobilidade
-- Dois públicos: TIME DE AUDIÊNCIA e EXPANSÃO DE LOJAS
-- Databricks SQL Editor → rode cada bloco → Add visualization → Dashboard
-- ============================================================================

USE CATALOG data_monetization;
USE SCHEMA gold;

-- ============================================================================
-- ██████████████████████████████████████████████████████
--   BLOCO A — TIME DE AUDIÊNCIA
--   Quem são as pessoas, como se comportam, quando e onde estão
-- ██████████████████████████████████████████████████████
-- ============================================================================

-- ----------------------------------------------------------------------------
-- A1: Perfil demográfico geral — quem visita os municípios
-- Visualização: Heatmap (genero x faixa_idade) colorido por total_visitas
-- Insight: mostra o público dominante de toda a base
-- ----------------------------------------------------------------------------
SELECT
    genero,
    faixa_idade,
    renda,
    SUM(qtd)                              AS total_visitas,
    ROUND(AVG(permanencia_minutos), 1)    AS permanencia_media_min,
    ROUND(SUM(qtd) * 100.0
        / SUM(SUM(qtd)) OVER (), 2)       AS pct_do_total
FROM fct_mobilidade
WHERE genero IS NOT NULL AND faixa_idade IS NOT NULL
GROUP BY genero, faixa_idade, renda
ORDER BY total_visitas DESC;

-- ----------------------------------------------------------------------------
-- A2: Distribuição por tipo de pessoa (Turista, Residente, Pendular etc.)
-- Visualização: Pie chart ou donut
-- Insight: entender se a audiência é local, flutuante ou turística
-- ----------------------------------------------------------------------------
SELECT
    tipo,
    SUM(qtd)                              AS total_visitas,
    ROUND(AVG(permanencia_minutos), 1)    AS permanencia_media_min,
    ROUND(SUM(qtd) * 100.0
        / SUM(SUM(qtd)) OVER (), 1)       AS pct_total
FROM fct_mobilidade
WHERE tipo IS NOT NULL
GROUP BY tipo
ORDER BY total_visitas DESC;

-- ----------------------------------------------------------------------------
-- A3: Comportamento por dia da semana
-- Visualização: Bar chart ordenado (Seg→Dom)
-- Insight: quando a audiência está ativa — orienta dias de campanha/ativação
-- ----------------------------------------------------------------------------
SELECT
    dia_semana,
    SUM(qtd)                              AS total_visitas,
    ROUND(AVG(permanencia_minutos), 1)    AS permanencia_media_min,
    COUNT(DISTINCT cod_municipio)         AS municipios_ativos
FROM fct_mobilidade
WHERE dia_semana IS NOT NULL
GROUP BY dia_semana
ORDER BY total_visitas DESC;

-- ----------------------------------------------------------------------------
-- A4: Duração da visita — quanto tempo ficam por faixa e tipo
-- Visualização: Stacked bar (tipo x faixa_duracao)
-- Insight: faixa_duracao + permanencia_minutos = profundidade do engajamento
-- ----------------------------------------------------------------------------
SELECT
    tipo,
    faixa_duracao,
    SUM(qtd)                              AS total_visitas,
    ROUND(AVG(permanencia_minutos), 1)    AS permanencia_media_min
FROM fct_mobilidade
WHERE tipo IS NOT NULL AND faixa_duracao IS NOT NULL
GROUP BY tipo, faixa_duracao
ORDER BY tipo, total_visitas DESC;

-- ----------------------------------------------------------------------------
-- A5: Personas dominantes por município
-- Visualização: Tabela com destaque
-- Insight: qual persona caracteriza cada território — base para segmentação
-- ----------------------------------------------------------------------------
WITH personas_ranked AS (
    SELECT
        nm_dist                               AS municipio,
        uf,
        personas,
        SUM(qtd)                              AS total_visitas,
        ROUND(AVG(permanencia_minutos), 1)    AS permanencia_media_min,
        ROW_NUMBER() OVER (
            PARTITION BY nm_dist
            ORDER BY SUM(qtd) DESC
        )                                     AS rn
    FROM fct_mobilidade
    WHERE personas IS NOT NULL AND nm_dist IS NOT NULL
    GROUP BY nm_dist, uf, personas
)
SELECT municipio, uf, personas, total_visitas, permanencia_media_min
FROM personas_ranked
WHERE rn = 1
ORDER BY total_visitas DESC
LIMIT 20;

-- ----------------------------------------------------------------------------
-- A6: Renda por região — poder aquisitivo da audiência por território
-- Visualização: Grouped bar (regiao x renda)
-- Insight: orienta segmentação de produto e precificação de mídia
-- ----------------------------------------------------------------------------
SELECT
    regiao,
    renda,
    SUM(qtd)                              AS total_visitas,
    ROUND(SUM(qtd) * 100.0
        / SUM(SUM(qtd)) OVER (PARTITION BY regiao), 1) AS pct_na_regiao
FROM fct_mobilidade
WHERE regiao IS NOT NULL AND renda IS NOT NULL
GROUP BY regiao, renda
ORDER BY regiao, total_visitas DESC;

-- ----------------------------------------------------------------------------
-- A7: Pernoites — audiência que dorme no município (alto valor para varejo)
-- Visualização: Bar + linha (qtd vs pernoites_reg)
-- Insight: quem pernoita tende a consumir mais — hotelaria, restaurante, loja
-- ----------------------------------------------------------------------------
SELECT
    nm_dist                               AS municipio,
    uf,
    SUM(qtd)                              AS total_visitas,
    ROUND(SUM(pernoites_reg), 0)          AS total_pernoites,
    ROUND(AVG(permanencia_minutos), 1)    AS permanencia_media_min,
    pernoites_banda_regiao
FROM fct_mobilidade
WHERE pernoites_reg IS NOT NULL AND nm_dist IS NOT NULL
GROUP BY nm_dist, uf, pernoites_banda_regiao
ORDER BY total_pernoites DESC
LIMIT 15;

-- ----------------------------------------------------------------------------
-- A8: Origem da audiência — de onde vêm as pessoas (fluxo moradia → município)
-- Visualização: Tabela OD (Origem-Destino)
-- Insight: mostra a bacia de captação de cada município visitado
-- ----------------------------------------------------------------------------
SELECT
    f.nm_dist                             AS municipio_visitado,
    f.uf                                  AS uf_destino,
    f.cod_cidade_moradia                  AS cod_origem_moradia,
    SUM(f.qtd)                            AS fluxo_visitas,
    ROUND(AVG(f.permanencia_minutos), 1)  AS permanencia_media_min
FROM fct_mobilidade f
WHERE f.cod_cidade_moradia IS NOT NULL
  AND f.nm_dist IS NOT NULL
  AND f.is_home_trip = FALSE
GROUP BY f.nm_dist, f.uf, f.cod_cidade_moradia
ORDER BY fluxo_visitas DESC
LIMIT 20;


-- ============================================================================
-- ██████████████████████████████████████████████████████
--   BLOCO B — EXPANSÃO DE LOJAS
--   Onde abrir, para quem, com qual potencial de captura
-- ██████████████████████████████████████████████████████
-- ============================================================================

-- ----------------------------------------------------------------------------
-- B1: Score de Atratividade por Município
-- Fórmula: combina volume (qtd), permanência e % de renda alta (A/B/C)
-- Visualização: Tabela ranqueada — esse é o slide principal para o gestor
-- Insight: "Top 10 municípios com maior potencial para nova loja"
-- ----------------------------------------------------------------------------
SELECT
    nm_dist                                           AS municipio,
    uf,
    regiao,
    SUM(qtd)                                          AS total_visitas,
    ROUND(AVG(permanencia_minutos), 1)                AS permanencia_media_min,
    -- % de visitas de renda alta (ajuste os valores conforme sua classificação)
    ROUND(
        SUM(CASE WHEN renda IN ('A','B','C') THEN qtd ELSE 0 END)
        * 100.0 / NULLIF(SUM(qtd), 0), 1
    )                                                 AS pct_renda_alta,
    -- Score composto (normalize para 100): volume 40% + permanência 40% + renda 20%
    ROUND(
        (SUM(qtd) / MAX(SUM(qtd)) OVER ()) * 40
        + (AVG(permanencia_minutos) / MAX(AVG(permanencia_minutos)) OVER ()) * 40
        + (SUM(CASE WHEN renda IN ('A','B','C') THEN qtd ELSE 0 END)
           / NULLIF(SUM(qtd), 0)) * 20
    , 1)                                              AS score_atratividade
FROM fct_mobilidade
WHERE nm_dist IS NOT NULL
GROUP BY nm_dist, uf, regiao
ORDER BY score_atratividade DESC
LIMIT 15;

-- ----------------------------------------------------------------------------
-- B2: Municípios com alta permanência (> média geral)
-- Visualização: Bar chart horizontal
-- Insight: permanência alta = mais tempo para consumir na loja
-- ----------------------------------------------------------------------------
WITH media AS (
    SELECT AVG(permanencia_minutos) AS media_geral FROM fct_mobilidade
)
SELECT
    f.nm_dist                             AS municipio,
    f.uf,
    SUM(f.qtd)                            AS total_visitas,
    ROUND(AVG(f.permanencia_minutos), 1)  AS permanencia_media_min,
    ROUND(m.media_geral, 1)               AS media_geral,
    ROUND(AVG(f.permanencia_minutos) - m.media_geral, 1) AS delta_vs_media
FROM fct_mobilidade f
CROSS JOIN media m
WHERE f.nm_dist IS NOT NULL
GROUP BY f.nm_dist, f.uf, m.media_geral
HAVING AVG(f.permanencia_minutos) > m.media_geral
ORDER BY delta_vs_media DESC
LIMIT 15;

-- ----------------------------------------------------------------------------
-- B3: Municípios com maior captação de trabalhadores (is_work_trip)
-- Visualização: Bar chart
-- Insight: alto fluxo de trabalho = ticket médio regular (alimentação, serviços)
-- ----------------------------------------------------------------------------
SELECT
    nm_dist                               AS municipio,
    uf,
    SUM(CASE WHEN is_work_trip THEN qtd ELSE 0 END)  AS visitas_trabalho,
    SUM(CASE WHEN is_home_trip THEN qtd ELSE 0 END)  AS visitas_moradia,
    SUM(qtd)                                          AS total_visitas,
    ROUND(
        SUM(CASE WHEN is_work_trip THEN qtd ELSE 0 END)
        * 100.0 / NULLIF(SUM(qtd), 0), 1
    )                                                 AS pct_trabalho
FROM fct_mobilidade
WHERE nm_dist IS NOT NULL
GROUP BY nm_dist, uf
ORDER BY visitas_trabalho DESC
LIMIT 15;

-- ----------------------------------------------------------------------------
-- B4: Potencial de expansão por dia da semana + município
-- Visualização: Heatmap (municipio x dia_semana), cor = qtd
-- Insight: onde há pico no fim de semana (lazer/varejo) vs dia útil (serviço)
-- ----------------------------------------------------------------------------
SELECT
    nm_dist                               AS municipio,
    uf,
    dia_semana,
    SUM(qtd)                              AS total_visitas,
    ROUND(AVG(permanencia_minutos), 1)    AS permanencia_media_min
FROM fct_mobilidade
WHERE nm_dist IS NOT NULL AND dia_semana IS NOT NULL
GROUP BY nm_dist, uf, dia_semana
ORDER BY nm_dist, total_visitas DESC;

-- ----------------------------------------------------------------------------
-- B5: Municípios com turismo relevante (tipo = Turista + pernoites)
-- Visualização: Scatter (eixo X = visitas turistas, eixo Y = pernoites)
-- Insight: municípios no quadrante alto-alto = destino turístico, alta margem
-- ----------------------------------------------------------------------------
SELECT
    nm_dist                                          AS municipio,
    uf,
    SUM(CASE WHEN tipo = 'Turista' THEN qtd ELSE 0 END) AS visitas_turista,
    ROUND(SUM(pernoites_reg), 0)                     AS total_pernoites,
    ROUND(AVG(permanencia_minutos), 1)               AS permanencia_media_min
FROM fct_mobilidade
WHERE nm_dist IS NOT NULL
GROUP BY nm_dist, uf
HAVING SUM(CASE WHEN tipo = 'Turista' THEN qtd ELSE 0 END) > 0
ORDER BY visitas_turista DESC
LIMIT 15;

-- ----------------------------------------------------------------------------
-- B6: Radar de Expansão — visão consolidada por município
-- Visualização: Tabela final de decisão (exportar para PPT/Slides)
-- Colunas: município, visitas totais, % turista, % renda alta,
--          permanência, pernoites, % trabalho — tudo em uma linha por praça
-- ----------------------------------------------------------------------------
SELECT
    nm_dist                                               AS municipio,
    uf,
    regiao,
    SUM(qtd)                                              AS total_visitas,
    ROUND(AVG(permanencia_minutos), 1)                    AS permanencia_media_min,
    ROUND(SUM(pernoites_reg), 0)                          AS total_pernoites,
    ROUND(SUM(CASE WHEN tipo = 'Turista' THEN qtd ELSE 0 END)
          * 100.0 / NULLIF(SUM(qtd), 0), 1)              AS pct_turista,
    ROUND(SUM(CASE WHEN renda IN ('A','B','C') THEN qtd ELSE 0 END)
          * 100.0 / NULLIF(SUM(qtd), 0), 1)              AS pct_renda_alta,
    ROUND(SUM(CASE WHEN is_work_trip THEN qtd ELSE 0 END)
          * 100.0 / NULLIF(SUM(qtd), 0), 1)              AS pct_trabalho,
    COUNT(DISTINCT dia_semana)                            AS dias_semana_ativos
FROM fct_mobilidade
WHERE nm_dist IS NOT NULL
GROUP BY nm_dist, uf, regiao
ORDER BY total_visitas DESC
LIMIT 20;

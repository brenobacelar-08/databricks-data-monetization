# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Criar Dashboard de Mobilidade via API
# MAGIC
# MAGIC Este notebook cria automaticamente o dashboard "Mobilidade — Audiência & Expansão"
# MAGIC no Databricks SQL, com todas as queries e visualizações prontas.
# MAGIC
# MAGIC **Rode este notebook uma única vez.** Depois acesse o dashboard em SQL → Dashboards.

# COMMAND ----------

import requests, json

# Pega host e token do contexto do notebook (sem precisar hardcodar credenciais)
ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
HOST  = "https://" + ctx.browserHostName().get()
TOKEN = ctx.apiToken().get()

HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
CATALOG = "data_monetization"
SCHEMA  = "gold"

print(f"Host: {HOST}")

# COMMAND ----------

# MAGIC %md ## 1. Descobrir o SQL Warehouse disponível

# COMMAND ----------

resp = requests.get(f"{HOST}/api/2.0/sql/warehouses", headers=HEADERS)
warehouses = resp.json().get("warehouses", [])
if not warehouses:
    raise Exception("Nenhum SQL Warehouse encontrado. Crie um em SQL → SQL Warehouses.")

# Usa o primeiro warehouse disponível (Serverless se existir)
warehouse = next(
    (w for w in warehouses if "serverless" in w.get("name","").lower()),
    warehouses[0]
)
WAREHOUSE_ID = warehouse["id"]
print(f"Warehouse: {warehouse['name']}  (id={WAREHOUSE_ID})")

# COMMAND ----------

# MAGIC %md ## 2. Definir queries e visualizações

# COMMAND ----------

QUERIES = [
    # ── AUDIÊNCIA ─────────────────────────────────────────────────────────────
    {
        "name": "A1 — Perfil Demográfico (Gênero × Idade × Renda)",
        "description": "Quem são as pessoas: heatmap de volume por gênero, faixa etária e renda",
        "query": f"""
SELECT genero, faixa_idade, renda,
       SUM(qtd) AS total_visitas,
       ROUND(AVG(permanencia_minutos),1) AS permanencia_media_min
FROM {CATALOG}.{SCHEMA}.fct_mobilidade
WHERE genero IS NOT NULL AND faixa_idade IS NOT NULL
GROUP BY genero, faixa_idade, renda
ORDER BY total_visitas DESC""",
        "viz_type": "TABLE",
    },
    {
        "name": "A2 — Tipo de Pessoa (Turista / Residente / Pendular)",
        "description": "Distribuição da audiência por tipo",
        "query": f"""
SELECT tipo,
       SUM(qtd) AS total_visitas,
       ROUND(AVG(permanencia_minutos),1) AS permanencia_media_min,
       ROUND(SUM(qtd)*100.0/SUM(SUM(qtd)) OVER (),1) AS pct_total
FROM {CATALOG}.{SCHEMA}.fct_mobilidade
WHERE tipo IS NOT NULL
GROUP BY tipo
ORDER BY total_visitas DESC""",
        "viz_type": "CHART",
        "viz_options": {"type": "pie", "columnMapping": {"x": "tipo", "y": "total_visitas"}},
    },
    {
        "name": "A3 — Volume por Dia da Semana",
        "description": "Quando a audiência está ativa — orienta dias de campanha",
        "query": f"""
SELECT dia_semana,
       SUM(qtd) AS total_visitas,
       ROUND(AVG(permanencia_minutos),1) AS permanencia_media_min
FROM {CATALOG}.{SCHEMA}.fct_mobilidade
WHERE dia_semana IS NOT NULL
GROUP BY dia_semana
ORDER BY total_visitas DESC""",
        "viz_type": "CHART",
        "viz_options": {"type": "bar", "columnMapping": {"x": "dia_semana", "y": "total_visitas"}},
    },
    {
        "name": "A4 — Duração da Visita por Tipo",
        "description": "Profundidade do engajamento: faixa_duracao por tipo de pessoa",
        "query": f"""
SELECT tipo, faixa_duracao,
       SUM(qtd) AS total_visitas,
       ROUND(AVG(permanencia_minutos),1) AS permanencia_media_min
FROM {CATALOG}.{SCHEMA}.fct_mobilidade
WHERE tipo IS NOT NULL AND faixa_duracao IS NOT NULL
GROUP BY tipo, faixa_duracao
ORDER BY tipo, total_visitas DESC""",
        "viz_type": "TABLE",
    },
    {
        "name": "A5 — Persona Dominante por Município",
        "description": "Qual persona caracteriza cada território",
        "query": f"""
WITH ranked AS (
  SELECT nm_dist AS municipio, uf, personas,
         SUM(qtd) AS total_visitas,
         ROUND(AVG(permanencia_minutos),1) AS permanencia_media_min,
         ROW_NUMBER() OVER (PARTITION BY nm_dist ORDER BY SUM(qtd) DESC) AS rn
  FROM {CATALOG}.{SCHEMA}.fct_mobilidade
  WHERE personas IS NOT NULL AND nm_dist IS NOT NULL
  GROUP BY nm_dist, uf, personas
)
SELECT municipio, uf, personas, total_visitas, permanencia_media_min
FROM ranked WHERE rn = 1
ORDER BY total_visitas DESC LIMIT 20""",
        "viz_type": "TABLE",
    },
    {
        "name": "A6 — Renda por Região",
        "description": "Poder aquisitivo da audiência por território",
        "query": f"""
SELECT regiao, renda,
       SUM(qtd) AS total_visitas,
       ROUND(SUM(qtd)*100.0/SUM(SUM(qtd)) OVER (PARTITION BY regiao),1) AS pct_na_regiao
FROM {CATALOG}.{SCHEMA}.fct_mobilidade
WHERE regiao IS NOT NULL AND renda IS NOT NULL
GROUP BY regiao, renda
ORDER BY regiao, total_visitas DESC""",
        "viz_type": "TABLE",
    },
    {
        "name": "A7 — Municípios com Mais Pernoites",
        "description": "Quem pernoita tende a consumir mais",
        "query": f"""
SELECT nm_dist AS municipio, uf,
       SUM(qtd) AS total_visitas,
       ROUND(SUM(pernoites_reg),0) AS total_pernoites,
       ROUND(AVG(permanencia_minutos),1) AS permanencia_media_min
FROM {CATALOG}.{SCHEMA}.fct_mobilidade
WHERE pernoites_reg IS NOT NULL AND nm_dist IS NOT NULL
GROUP BY nm_dist, uf
ORDER BY total_pernoites DESC LIMIT 15""",
        "viz_type": "CHART",
        "viz_options": {"type": "bar", "columnMapping": {"x": "municipio", "y": "total_pernoites"}},
    },
    # ── EXPANSÃO ──────────────────────────────────────────────────────────────
    {
        "name": "B1 — Score de Atratividade por Município",
        "description": "Ranking: volume + permanência + renda combinados — slide principal para o gestor",
        "query": f"""
SELECT nm_dist AS municipio, uf, regiao,
       SUM(qtd) AS total_visitas,
       ROUND(AVG(permanencia_minutos),1) AS permanencia_media_min,
       ROUND(SUM(CASE WHEN renda IN ('A','B','C') THEN qtd ELSE 0 END)*100.0/NULLIF(SUM(qtd),0),1) AS pct_renda_alta,
       ROUND(
         (SUM(qtd)/MAX(SUM(qtd)) OVER ())*40
         +(AVG(permanencia_minutos)/MAX(AVG(permanencia_minutos)) OVER ())*40
         +(SUM(CASE WHEN renda IN ('A','B','C') THEN qtd ELSE 0 END)/NULLIF(SUM(qtd),0))*20
       ,1) AS score_atratividade
FROM {CATALOG}.{SCHEMA}.fct_mobilidade
WHERE nm_dist IS NOT NULL
GROUP BY nm_dist, uf, regiao
ORDER BY score_atratividade DESC LIMIT 15""",
        "viz_type": "TABLE",
    },
    {
        "name": "B2 — Permanência Acima da Média",
        "description": "Municípios onde as pessoas ficam mais tempo (maior potencial de consumo)",
        "query": f"""
WITH media AS (SELECT AVG(permanencia_minutos) AS media_geral FROM {CATALOG}.{SCHEMA}.fct_mobilidade)
SELECT f.nm_dist AS municipio, f.uf,
       SUM(f.qtd) AS total_visitas,
       ROUND(AVG(f.permanencia_minutos),1) AS permanencia_media_min,
       ROUND(m.media_geral,1) AS media_geral,
       ROUND(AVG(f.permanencia_minutos)-m.media_geral,1) AS delta_vs_media
FROM {CATALOG}.{SCHEMA}.fct_mobilidade f CROSS JOIN media m
WHERE f.nm_dist IS NOT NULL
GROUP BY f.nm_dist, f.uf, m.media_geral
HAVING AVG(f.permanencia_minutos) > m.media_geral
ORDER BY delta_vs_media DESC LIMIT 15""",
        "viz_type": "CHART",
        "viz_options": {"type": "bar", "columnMapping": {"x": "municipio", "y": "delta_vs_media"}},
    },
    {
        "name": "B3 — Captação de Trabalhadores por Município",
        "description": "Municípios que atraem mais trabalhadores (fluxo pendular)",
        "query": f"""
SELECT nm_dist AS municipio, uf,
       SUM(CASE WHEN is_work_trip THEN qtd ELSE 0 END) AS visitas_trabalho,
       SUM(CASE WHEN is_home_trip THEN qtd ELSE 0 END) AS visitas_moradia,
       SUM(qtd) AS total_visitas,
       ROUND(SUM(CASE WHEN is_work_trip THEN qtd ELSE 0 END)*100.0/NULLIF(SUM(qtd),0),1) AS pct_trabalho
FROM {CATALOG}.{SCHEMA}.fct_mobilidade
WHERE nm_dist IS NOT NULL
GROUP BY nm_dist, uf
ORDER BY visitas_trabalho DESC LIMIT 15""",
        "viz_type": "TABLE",
    },
    {
        "name": "B4 — Padrão por Dia da Semana × Município",
        "description": "Heatmap: onde há pico no fim de semana vs dia útil",
        "query": f"""
SELECT nm_dist AS municipio, uf, dia_semana,
       SUM(qtd) AS total_visitas,
       ROUND(AVG(permanencia_minutos),1) AS permanencia_media_min
FROM {CATALOG}.{SCHEMA}.fct_mobilidade
WHERE nm_dist IS NOT NULL AND dia_semana IS NOT NULL
GROUP BY nm_dist, uf, dia_semana
ORDER BY nm_dist, total_visitas DESC""",
        "viz_type": "TABLE",
    },
    {
        "name": "B5 — Municípios Turísticos (Visitas × Pernoites)",
        "description": "Alto turismo + pernoites = alto potencial de consumo e margem",
        "query": f"""
SELECT nm_dist AS municipio, uf,
       SUM(CASE WHEN tipo='Turista' THEN qtd ELSE 0 END) AS visitas_turista,
       ROUND(SUM(pernoites_reg),0) AS total_pernoites,
       ROUND(AVG(permanencia_minutos),1) AS permanencia_media_min
FROM {CATALOG}.{SCHEMA}.fct_mobilidade
WHERE nm_dist IS NOT NULL
GROUP BY nm_dist, uf
HAVING SUM(CASE WHEN tipo='Turista' THEN qtd ELSE 0 END) > 0
ORDER BY visitas_turista DESC LIMIT 15""",
        "viz_type": "CHART",
        "viz_options": {"type": "bar", "columnMapping": {"x": "municipio", "y": "visitas_turista"}},
    },
    {
        "name": "B6 — Radar de Expansão (Visão Consolidada)",
        "description": "Tabela completa de decisão: todos os indicadores por município em uma linha",
        "query": f"""
SELECT nm_dist AS municipio, uf, regiao,
       SUM(qtd) AS total_visitas,
       ROUND(AVG(permanencia_minutos),1) AS permanencia_media_min,
       ROUND(SUM(pernoites_reg),0) AS total_pernoites,
       ROUND(SUM(CASE WHEN tipo='Turista' THEN qtd ELSE 0 END)*100.0/NULLIF(SUM(qtd),0),1) AS pct_turista,
       ROUND(SUM(CASE WHEN renda IN ('A','B','C') THEN qtd ELSE 0 END)*100.0/NULLIF(SUM(qtd),0),1) AS pct_renda_alta,
       ROUND(SUM(CASE WHEN is_work_trip THEN qtd ELSE 0 END)*100.0/NULLIF(SUM(qtd),0),1) AS pct_trabalho,
       COUNT(DISTINCT dia_semana) AS dias_semana_ativos
FROM {CATALOG}.{SCHEMA}.fct_mobilidade
WHERE nm_dist IS NOT NULL
GROUP BY nm_dist, uf, regiao
ORDER BY total_visitas DESC LIMIT 20""",
        "viz_type": "TABLE",
    },
]

# COMMAND ----------

# MAGIC %md ## 3. Criar queries no Databricks SQL

# COMMAND ----------

created_queries = []

for q in QUERIES:
    payload = {
        "name":         q["name"],
        "description":  q.get("description", ""),
        "query":        q["query"],
        "data_source_id": WAREHOUSE_ID,
    }
    r = requests.post(f"{HOST}/api/2.0/preview/sql/queries", headers=HEADERS, json=payload)
    if r.status_code == 200:
        data = r.json()
        created_queries.append({"id": data["id"], "name": q["name"], "viz_type": q.get("viz_type","TABLE"), "viz_options": q.get("viz_options",{})})
        print(f"  ✓ Query criada: {q['name'][:60]}")
    else:
        print(f"  ✗ Erro em '{q['name']}': {r.status_code} — {r.text[:120]}")

print(f"\nTotal: {len(created_queries)} queries criadas")

# COMMAND ----------

# MAGIC %md ## 4. Criar visualizações para cada query

# COMMAND ----------

viz_ids = []

for q in created_queries:
    if q["viz_type"] == "CHART" and q.get("viz_options"):
        opts = q["viz_options"]
        viz_payload = {
            "type":          opts.get("type", "bar").upper(),
            "name":          q["name"],
            "query_id":      q["id"],
            "description":   "",
            "options": {
                "globalSeriesType": opts.get("type", "bar"),
                "columnMapping":    opts.get("columnMapping", {}),
            }
        }
    else:
        viz_payload = {
            "type":     "TABLE",
            "name":     q["name"],
            "query_id": q["id"],
            "options":  {},
        }

    r = requests.post(f"{HOST}/api/2.0/preview/sql/visualizations", headers=HEADERS, json=viz_payload)
    if r.status_code == 200:
        viz_ids.append({"viz_id": r.json()["id"], "name": q["name"]})
        print(f"  ✓ Viz: {q['name'][:55]}")
    else:
        print(f"  ✗ {q['name'][:55]}: {r.text[:100]}")

# COMMAND ----------

# MAGIC %md ## 5. Criar o Dashboard e adicionar widgets

# COMMAND ----------

dash_r = requests.post(
    f"{HOST}/api/2.0/preview/sql/dashboards",
    headers=HEADERS,
    json={"name": "Mobilidade — Audiência & Expansão de Lojas"}
)
DASHBOARD_ID = dash_r.json()["id"]
print(f"Dashboard criado: id={DASHBOARD_ID}")

# Adiciona cada visualização como widget no dashboard
for i, v in enumerate(viz_ids):
    widget_payload = {
        "dashboard_id":    DASHBOARD_ID,
        "visualization_id": v["viz_id"],
        "options":          {"position": {"col": (i % 2) * 3, "row": i // 2 * 4, "sizeX": 3, "sizeY": 4}},
        "text":             "",
    }
    requests.post(f"{HOST}/api/2.0/preview/sql/widgets", headers=HEADERS, json=widget_payload)

print(f"\n✅ Dashboard pronto!")
print(f"Acesse em: SQL → Dashboards → 'Mobilidade — Audiência & Expansão de Lojas'")
print(f"Ou direto: {HOST}/sql/dashboards/{DASHBOARD_ID}")

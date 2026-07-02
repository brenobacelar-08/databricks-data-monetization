# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Criar Dashboard Lakeview (AI/BI) via API
# MAGIC
# MAGIC Cria o dashboard "Mobilidade — Audiência & Expansão de Lojas"
# MAGIC usando a API Lakeview (`/api/2.0/lakeview/dashboards`),
# MAGIC que é a única suportada neste workspace (Free Edition).

# COMMAND ----------

import requests, json, uuid

ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
HOST  = "https://" + ctx.browserHostName().get()
TOKEN = ctx.apiToken().get()
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

CATALOG = "data_monetization"
SCHEMA  = "gold"

print(f"Host: {HOST}")

# COMMAND ----------

# MAGIC %md ## 1. Definir queries e tipo de visualização

# COMMAND ----------

QUERIES = [
    # ── AUDIÊNCIA ────────────────────────────────────────────────────────
    {
        "name": "A1 — Perfil Demográfico",
        "description": "Volume por gênero, faixa etária e renda",
        "query": f"SELECT genero, faixa_idade, renda, SUM(qtd) AS total_visitas, ROUND(AVG(permanencia_minutos),1) AS permanencia_media_min FROM {CATALOG}.{SCHEMA}.fct_mobilidade WHERE genero IS NOT NULL AND faixa_idade IS NOT NULL GROUP BY genero, faixa_idade, renda ORDER BY total_visitas DESC",
        "widget_type": "table",
    },
    {
        "name": "A2 — Tipo de Pessoa",
        "description": "Distribuição: Turista / Residente / Pendular",
        "query": f"SELECT tipo, SUM(qtd) AS total_visitas, ROUND(AVG(permanencia_minutos),1) AS permanencia_media_min, ROUND(SUM(qtd)*100.0/SUM(SUM(qtd)) OVER (),1) AS pct_total FROM {CATALOG}.{SCHEMA}.fct_mobilidade WHERE tipo IS NOT NULL GROUP BY tipo ORDER BY total_visitas DESC",
        "widget_type": "bar",
        "x_col": "tipo", "y_col": "total_visitas",
    },
    {
        "name": "A3 — Volume por Dia da Semana",
        "description": "Quando a audiência está mais ativa",
        "query": f"SELECT dia_semana, SUM(qtd) AS total_visitas, ROUND(AVG(permanencia_minutos),1) AS permanencia_media_min FROM {CATALOG}.{SCHEMA}.fct_mobilidade WHERE dia_semana IS NOT NULL GROUP BY dia_semana ORDER BY total_visitas DESC",
        "widget_type": "bar",
        "x_col": "dia_semana", "y_col": "total_visitas",
    },
    {
        "name": "A4 — Duração da Visita por Tipo",
        "description": "Faixa de duração cruzada com tipo de pessoa",
        "query": f"SELECT tipo, faixa_duracao, SUM(qtd) AS total_visitas, ROUND(AVG(permanencia_minutos),1) AS permanencia_media_min FROM {CATALOG}.{SCHEMA}.fct_mobilidade WHERE tipo IS NOT NULL AND faixa_duracao IS NOT NULL GROUP BY tipo, faixa_duracao ORDER BY tipo, total_visitas DESC",
        "widget_type": "table",
    },
    {
        "name": "A5 — Persona Dominante por Município",
        "description": "Qual persona caracteriza cada território",
        "query": f"WITH r AS (SELECT nm_dist AS municipio, uf, personas, SUM(qtd) AS total_visitas, ROUND(AVG(permanencia_minutos),1) AS permanencia_media_min, ROW_NUMBER() OVER (PARTITION BY nm_dist ORDER BY SUM(qtd) DESC) AS rn FROM {CATALOG}.{SCHEMA}.fct_mobilidade WHERE personas IS NOT NULL AND nm_dist IS NOT NULL GROUP BY nm_dist, uf, personas) SELECT municipio, uf, personas, total_visitas, permanencia_media_min FROM r WHERE rn=1 ORDER BY total_visitas DESC LIMIT 20",
        "widget_type": "table",
    },
    {
        "name": "A6 — Renda por Região",
        "description": "Poder aquisitivo da audiência por território",
        "query": f"SELECT regiao, renda, SUM(qtd) AS total_visitas, ROUND(SUM(qtd)*100.0/SUM(SUM(qtd)) OVER (PARTITION BY regiao),1) AS pct_na_regiao FROM {CATALOG}.{SCHEMA}.fct_mobilidade WHERE regiao IS NOT NULL AND renda IS NOT NULL GROUP BY regiao, renda ORDER BY regiao, total_visitas DESC",
        "widget_type": "bar",
        "x_col": "renda", "y_col": "total_visitas",
    },
    {
        "name": "A7 — Municípios com Mais Pernoites",
        "description": "Quem pernoita tende a consumir mais",
        "query": f"SELECT nm_dist AS municipio, uf, SUM(qtd) AS total_visitas, ROUND(SUM(pernoites_reg),0) AS total_pernoites, ROUND(AVG(permanencia_minutos),1) AS permanencia_media_min FROM {CATALOG}.{SCHEMA}.fct_mobilidade WHERE pernoites_reg IS NOT NULL AND nm_dist IS NOT NULL GROUP BY nm_dist, uf ORDER BY total_pernoites DESC LIMIT 15",
        "widget_type": "bar",
        "x_col": "municipio", "y_col": "total_pernoites",
    },
    # ── EXPANSÃO ─────────────────────────────────────────────────────────
    {
        "name": "B1 — Score de Atratividade",
        "description": "Ranking: volume + permanência + renda — slide principal para o gestor",
        "query": f"SELECT nm_dist AS municipio, uf, regiao, SUM(qtd) AS total_visitas, ROUND(AVG(permanencia_minutos),1) AS permanencia_media_min, ROUND(SUM(CASE WHEN renda IN ('A','B','C') THEN qtd ELSE 0 END)*100.0/NULLIF(SUM(qtd),0),1) AS pct_renda_alta, ROUND((SUM(qtd)/MAX(SUM(qtd)) OVER ())*40+(AVG(permanencia_minutos)/MAX(AVG(permanencia_minutos)) OVER ())*40+(SUM(CASE WHEN renda IN ('A','B','C') THEN qtd ELSE 0 END)/NULLIF(SUM(qtd),0))*20,1) AS score_atratividade FROM {CATALOG}.{SCHEMA}.fct_mobilidade WHERE nm_dist IS NOT NULL GROUP BY nm_dist, uf, regiao ORDER BY score_atratividade DESC LIMIT 15",
        "widget_type": "bar",
        "x_col": "municipio", "y_col": "score_atratividade",
    },
    {
        "name": "B2 — Permanência Acima da Média",
        "description": "Onde as pessoas ficam mais tempo que a média geral",
        "query": f"WITH m AS (SELECT AVG(permanencia_minutos) AS mg FROM {CATALOG}.{SCHEMA}.fct_mobilidade) SELECT f.nm_dist AS municipio, f.uf, SUM(f.qtd) AS total_visitas, ROUND(AVG(f.permanencia_minutos),1) AS permanencia_media_min, ROUND(AVG(f.permanencia_minutos)-m.mg,1) AS delta_vs_media FROM {CATALOG}.{SCHEMA}.fct_mobilidade f CROSS JOIN m WHERE f.nm_dist IS NOT NULL GROUP BY f.nm_dist, f.uf, m.mg HAVING AVG(f.permanencia_minutos)>m.mg ORDER BY delta_vs_media DESC LIMIT 15",
        "widget_type": "bar",
        "x_col": "municipio", "y_col": "delta_vs_media",
    },
    {
        "name": "B3 — Captação de Trabalhadores",
        "description": "Municípios com mais fluxo de trabalho (pendular)",
        "query": f"SELECT nm_dist AS municipio, uf, SUM(CASE WHEN is_work_trip THEN qtd ELSE 0 END) AS visitas_trabalho, SUM(CASE WHEN is_home_trip THEN qtd ELSE 0 END) AS visitas_moradia, SUM(qtd) AS total_visitas, ROUND(SUM(CASE WHEN is_work_trip THEN qtd ELSE 0 END)*100.0/NULLIF(SUM(qtd),0),1) AS pct_trabalho FROM {CATALOG}.{SCHEMA}.fct_mobilidade WHERE nm_dist IS NOT NULL GROUP BY nm_dist, uf ORDER BY visitas_trabalho DESC LIMIT 15",
        "widget_type": "bar",
        "x_col": "municipio", "y_col": "visitas_trabalho",
    },
    {
        "name": "B4 — Municípios Turísticos",
        "description": "Visitas de turistas × pernoites por município",
        "query": f"SELECT nm_dist AS municipio, uf, SUM(CASE WHEN tipo='Turista' THEN qtd ELSE 0 END) AS visitas_turista, ROUND(SUM(pernoites_reg),0) AS total_pernoites, ROUND(AVG(permanencia_minutos),1) AS permanencia_media_min FROM {CATALOG}.{SCHEMA}.fct_mobilidade WHERE nm_dist IS NOT NULL GROUP BY nm_dist, uf HAVING SUM(CASE WHEN tipo='Turista' THEN qtd ELSE 0 END)>0 ORDER BY visitas_turista DESC LIMIT 15",
        "widget_type": "bar",
        "x_col": "municipio", "y_col": "visitas_turista",
    },
    {
        "name": "B5 — Radar de Expansão (Visão Consolidada)",
        "description": "Tabela completa de decisão: todos os indicadores por município",
        "query": f"SELECT nm_dist AS municipio, uf, regiao, SUM(qtd) AS total_visitas, ROUND(AVG(permanencia_minutos),1) AS permanencia_media_min, ROUND(SUM(pernoites_reg),0) AS total_pernoites, ROUND(SUM(CASE WHEN tipo='Turista' THEN qtd ELSE 0 END)*100.0/NULLIF(SUM(qtd),0),1) AS pct_turista, ROUND(SUM(CASE WHEN renda IN ('A','B','C') THEN qtd ELSE 0 END)*100.0/NULLIF(SUM(qtd),0),1) AS pct_renda_alta, ROUND(SUM(CASE WHEN is_work_trip THEN qtd ELSE 0 END)*100.0/NULLIF(SUM(qtd),0),1) AS pct_trabalho FROM {CATALOG}.{SCHEMA}.fct_mobilidade WHERE nm_dist IS NOT NULL GROUP BY nm_dist, uf, regiao ORDER BY total_visitas DESC LIMIT 20",
        "widget_type": "table",
    },
]

# COMMAND ----------

# MAGIC %md ## 2. Montar o serialized_dashboard (formato Lakeview)

# COMMAND ----------

page_name = str(uuid.uuid4())
datasets  = []
layout    = []

for i, q in enumerate(QUERIES):
    ds_name     = str(uuid.uuid4())
    widget_name = str(uuid.uuid4())
    query_ref   = str(uuid.uuid4())

    datasets.append({
        "name":        ds_name,
        "displayName": q["name"],
        "query":       q["query"],
    })

    x_col = q.get("x_col", "")
    y_col = q.get("y_col", "")

    if q["widget_type"] == "bar":
        query_name = "main_query"   # nome fixo — referenciado em spec.data.queryName
        spec = {
            "version":    3,
            "widgetType": "bar",
            "frame":      {"showTitle": True, "title": q["name"]},
            # data.queryName é o vínculo entre spec e o dataset — campo obrigatório
            "data":       {"queryName": query_name},
            "encodings":  {
                "x": {"fieldName": x_col},
                "y": [{"fieldName": y_col}],
            },
        }
        disaggregated = False
    else:
        query_name    = str(uuid.uuid4())
        disaggregated = True
        spec = {
            "version":    3,
            "widgetType": "table",
            "frame":      {"showTitle": True, "title": q["name"]},
            # tabelas não precisam de data/encodings
        }

    col = (i % 2) * 6
    row = (i // 2) * 7

    layout.append({
        "widget": {
            "name": widget_name,
            # queries: sem fields — só datasetName + disaggregated
            "queries": [{
                "name":  query_name,
                "query": {
                    "datasetName":   ds_name,
                    "disaggregated": disaggregated,
                },
            }],
            "spec": spec,
        },
        "position": {"x": col, "y": row, "width": 6, "height": 6},
    })

serialized = json.dumps({
    "pages": [{
        "name":        page_name,
        "displayName": "Mobilidade",
        "layout":      layout,
    }],
    "datasets": datasets,
})

print(f"Datasets  : {len(datasets)}")
print(f"Widgets   : {len(widgets)}")

# COMMAND ----------

# MAGIC %md ## 3. Apagar dashboard anterior (se existir) e recriar

# COMMAND ----------

# Lista dashboards existentes e apaga o que tem o mesmo nome
list_r = requests.get(f"{HOST}/api/2.0/lakeview/dashboards", headers=HEADERS)
for d in list_r.json().get("dashboards", []):
    if d.get("display_name") == "Mobilidade — Audiência & Expansão de Lojas":
        did = d.get("dashboard_id") or d.get("id")
        requests.delete(f"{HOST}/api/2.0/lakeview/dashboards/{did}", headers=HEADERS)
        print(f"Dashboard anterior removido: {did}")

# COMMAND ----------

resp = requests.post(
    f"{HOST}/api/2.0/lakeview/dashboards",
    headers=HEADERS,
    json={
        "display_name":         "Mobilidade — Audiência & Expansão de Lojas",
        "serialized_dashboard": serialized,
    }
)

print(f"Status : {resp.status_code}")

if resp.status_code not in (200, 201):
    print("Resposta de erro:")
    print(json.dumps(resp.json(), indent=2, ensure_ascii=False)[:1000])
    raise Exception("Falha ao criar dashboard — veja a mensagem acima")

dash = resp.json()
DASHBOARD_ID = dash.get("dashboard_id") or dash.get("id")

print(f"\n✅ Dashboard criado com sucesso!")
print(f"ID      : {DASHBOARD_ID}")
print(f"Acesse  : {HOST}/dashboards/{DASHBOARD_ID}")
print(f"\nOu via menu: SQL → Dashboards → 'Mobilidade — Audiência & Expansão de Lojas'")

# Databricks notebook source
# MAGIC %md
# MAGIC # 05b — Debug: inspecionar formato real do dashboard Lakeview
# MAGIC
# MAGIC Roda este notebook para ver o JSON exato que o Databricks usa internamente.
# MAGIC Depois usamos esse formato para corrigir o 05_create_dashboard.

# COMMAND ----------

import requests, json

ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
HOST  = "https://" + ctx.browserHostName().get()
TOKEN = ctx.apiToken().get()
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# COMMAND ----------

# MAGIC %md ## 1. Listar todos os dashboards Lakeview

# COMMAND ----------

r = requests.get(f"{HOST}/api/2.0/lakeview/dashboards", headers=HEADERS)
dashboards = r.json().get("dashboards", [])
for d in dashboards:
    print(f"id={d.get('dashboard_id') or d.get('id')}  nome={d.get('display_name')}")

# COMMAND ----------

# MAGIC %md ## 2. Buscar o serialized_dashboard do nosso dashboard

# COMMAND ----------

# Pega o ID do dashboard "Mobilidade"
target = next((d for d in dashboards if "Mobilidade" in d.get("display_name","")), None)

if not target:
    print("Dashboard não encontrado — rode o 05_create_dashboard primeiro")
else:
    did = target.get("dashboard_id") or target.get("id")
    r2  = requests.get(f"{HOST}/api/2.0/lakeview/dashboards/{did}", headers=HEADERS)
    dash_json = r2.json()

    sd     = dash_json.get("serialized_dashboard", "{}")
    parsed = json.loads(sd) if isinstance(sd, str) else sd

    # Mostra só os primeiros 2 widgets do layout (o que interessa para o formato)
    pages = parsed.get("pages", [])
    if pages:
        layout = pages[0].get("layout", [])
        print("=== PRIMEIROS 2 WIDGETS DO LAYOUT ===")
        print(json.dumps(layout[:2], indent=2, ensure_ascii=False))

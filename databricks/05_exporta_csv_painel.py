# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Exporta o CSV da Ouro para o painel (requisito 08)
# MAGIC
# MAGIC O Databricks Free Edition não se conecta ao painel, então a entrega é por arquivo:
# MAGIC
# MAGIC 1. Este notebook grava `vw_painel_indicadores` em `chegada/_export/indicadores_ouro.csv` no Volume.
# MAGIC 2. Baixe o arquivo pela interface (Catalog > Volume > `_export` > Download).
# MAGIC 3. Entregue o CSV ao responsável pelo painel (DevOps), que o carrega na tela de indicadores.
# MAGIC
# MAGIC Colunas do CSV (contrato com o painel):
# MAGIC `loja_id, equipamento_id, categoria, data, periodo, tipo, eventos, eventos_acima_do_limite, duracao_total_s,
# MAGIC duracao_media_s, kwh_desperdicado, custo_estimado_rs, custo_aneel_rs, bandeira, temp_max_c`
# MAGIC
# MAGIC Não há dado pessoal no arquivo.

# COMMAND ----------

CATALOGO = "workspace"
ESQUEMA = "energia_lucro"
PASTA = f"/Volumes/{CATALOGO}/{ESQUEMA}/chegada/_export"
DESTINO = f"{PASTA}/indicadores_ouro.csv"

df = spark.table(f"{CATALOGO}.{ESQUEMA}.vw_painel_indicadores").orderBy("data", "loja_id", "equipamento_id", "tipo")

dbutils.fs.mkdirs(PASTA)
pdf = df.toPandas()
pdf.to_csv(DESTINO, index=False, encoding="utf-8")

print(f"{len(pdf)} linhas gravadas em {DESTINO}")
print(f"Período: {pdf['data'].min()} a {pdf['data'].max()} | lojas: {sorted(pdf['loja_id'].unique())}")

# COMMAND ----------

display(df.limit(20))

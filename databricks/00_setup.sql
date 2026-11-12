-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Setup — Energia que dá Lucro
-- MAGIC Cria o esquema e o Volume de chegada. Rodar uma vez.
-- MAGIC
-- MAGIC Depois de rodar, envie os arquivos **pela interface** (Catalog > workspace > energia_lucro > chegada > *Upload to this volume*):
-- MAGIC
-- MAGIC | Destino no Volume | O que enviar | Quando |
-- MAGIC |---|---|---|
-- MAGIC | raiz | `lojas.csv` e `equipamentos.csv` (de `amostras/cadastro/`) | uma vez |
-- MAGIC | `lote1/clima`, `lote1/bandeiras`, `lote1/tarifas`, `lote1/eventos` | arquivos dos dias do **1º lote** | 1º lote |
-- MAGIC | `lote2/...` (mesmas quatro subpastas) | arquivos dos dias seguintes | **2º lote**, depois de rodar a Bronze uma vez |
-- MAGIC
-- MAGIC O Databricks Free Edition não se conecta ao Airflow, por isso o envio é manual, em lotes.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.energia_lucro
COMMENT 'GS 2026.2 - Energia que dá Lucro - componente Dados';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.energia_lucro.chegada
COMMENT 'Pasta de pouso: arquivos enviados da pasta chegada/ do Airflow';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Conferência: depois do upload, esta listagem deve mostrar as subpastas.

-- COMMAND ----------

LIST '/Volumes/workspace/energia_lucro/chegada/';

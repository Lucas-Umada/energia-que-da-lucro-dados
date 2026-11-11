# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Ouro — modelo dimensional e LGPD (requisitos 07 e 08)
# MAGIC
# MAGIC | Tabela | Papel | Grão |
# MAGIC |---|---|---|
# MAGIC | `fato_desperdicio` | fato | um evento de desperdício |
# MAGIC | `dim_loja` | dimensão | uma loja, **com os dados pessoais protegidos** |
# MAGIC | `dim_equipamento` | dimensão | um equipamento de uma loja |
# MAGIC | `dim_tempo` | dimensão | um dia, com a bandeira tarifária e o clima do dia |
# MAGIC | `vw_painel_indicadores` | visão | loja × equipamento × dia — **é o que o painel do MVP consome** |
# MAGIC
# MAGIC A Ouro é refeita inteira a cada execução (`CREATE OR REPLACE`) a partir da Prata, que já está sem duplicatas.
# MAGIC
# MAGIC **LGPD:** o único dado pessoal do produto é o cadastro do responsável pela loja (nome, e-mail, telefone).
# MAGIC Na Ouro ele aparece só como hash com sal (para contar/relacionar sem identificar) e como máscara (para exibir).

# COMMAND ----------

dbutils.widgets.text("sal_hash", "", "Sal do hash (não versionar)")
SAL = dbutils.widgets.get("sal_hash")
if len(SAL) < 12:
    raise ValueError(
        "Preencha o widget 'sal_hash' com um texto aleatório de 12+ caracteres. "
        "O sal não vai para o repositório: guarde-o fora do código."
    )
SAL_SQL = SAL.replace("'", "''")

spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA energia_lucro")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_loja — dados pessoais com hash e máscara

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE dim_loja AS
SELECT
  dense_rank() OVER (ORDER BY loja_id)                                         AS loja_sk,
  loja_id,
  nome_fantasia,
  tipo,
  cidade,
  uf,
  hora_abre,
  hora_fecha,
  data_inicio_alertas,
  sha2(concat('{SAL_SQL}', responsavel_email), 256)                            AS responsavel_hash,
  concat(left(responsavel_nome, 1), '***')                                     AS responsavel_nome_mascarado,
  concat(left(responsavel_email, 1), '***@', split_part(responsavel_email, '@', 2)) AS responsavel_email_mascarado,
  concat('(**) *****-', right(regexp_replace(responsavel_telefone, '[^0-9]', ''), 4)) AS responsavel_telefone_mascarado
FROM prata_lojas
""")
display(spark.table("dim_loja"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_equipamento

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_equipamento AS
# MAGIC SELECT
# MAGIC   dense_rank() OVER (ORDER BY loja_id, equipamento_id) AS equipamento_sk,
# MAGIC   loja_id,
# MAGIC   equipamento_id,
# MAGIC   categoria,
# MAGIC   potencia_kw,
# MAGIC   fator_perda
# MAGIC FROM prata_equipamentos;
# MAGIC
# MAGIC SELECT * FROM dim_equipamento ORDER BY equipamento_sk;

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_tempo — um dia, com bandeira tarifária (ANEEL) e clima (Open-Meteo)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_tempo AS
# MAGIC WITH dias AS (
# MAGIC   SELECT DISTINCT to_date(inicio_local) AS data FROM prata_eventos
# MAGIC   UNION
# MAGIC   SELECT DISTINCT to_date(data_hora) FROM prata_clima
# MAGIC ),
# MAGIC clima_dia AS (
# MAGIC   SELECT to_date(data_hora) AS data,
# MAGIC          round(max(temperatura_c), 1) AS temp_max_c,
# MAGIC          round(avg(temperatura_c), 1) AS temp_media_c
# MAGIC   FROM prata_clima
# MAGIC   GROUP BY to_date(data_hora)
# MAGIC )
# MAGIC SELECT
# MAGIC   CAST(date_format(d.data, 'yyyyMMdd') AS INT)   AS data_id,
# MAGIC   d.data,
# MAGIC   year(d.data)                                   AS ano,
# MAGIC   month(d.data)                                  AS mes,
# MAGIC   day(d.data)                                    AS dia,
# MAGIC   date_format(d.data, 'yyyy-MM')                 AS periodo,
# MAGIC   dayofweek(d.data)                              AS dia_semana,
# MAGIC   dayofweek(d.data) IN (1, 7)                    AS fim_de_semana,
# MAGIC   b.bandeira,
# MAGIC   coalesce(b.adicional_rs_mwh, 0) / 1000         AS adicional_bandeira_rs_kwh,
# MAGIC   c.temp_max_c,
# MAGIC   c.temp_media_c
# MAGIC FROM dias d
# MAGIC LEFT JOIN prata_bandeiras b ON b.competencia = trunc(d.data, 'MM')
# MAGIC LEFT JOIN clima_dia c ON c.data = d.data
# MAGIC WHERE d.data IS NOT NULL;
# MAGIC
# MAGIC SELECT * FROM dim_tempo ORDER BY data_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## fato_desperdicio
# MAGIC Duas medidas de custo:
# MAGIC - `custo_estimado_rs`: veio no evento, pela fórmula do contrato do grupo (tarifa provisória de R$ 0,90/kWh).
# MAGIC - `custo_aneel_rs`: mesmo kWh, valorizado pela tarifa B3 homologada da distribuidora + adicional da bandeira do mês,
# MAGIC   ambos da ANEEL. **Sem tributos.**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fato_desperdicio AS
# MAGIC WITH tarifa AS (
# MAGIC   -- uma tarifa por vigência (a distribuidora das lojas fictícias é uma só)
# MAGIC   SELECT inicio_vigencia, fim_vigencia, tarifa_rs_kwh
# MAGIC   FROM prata_tarifas
# MAGIC   QUALIFY row_number() OVER (PARTITION BY inicio_vigencia ORDER BY carregado_em DESC) = 1
# MAGIC ),
# MAGIC base AS (
# MAGIC   SELECT
# MAGIC     e.id                                                         AS evento_id,
# MAGIC     l.loja_sk,
# MAGIC     q.equipamento_sk,
# MAGIC     t.data_id,
# MAGIC     hour(e.inicio_local)                                         AS hora,
# MAGIC     date_trunc('HOUR', e.inicio_local)                           AS hora_referencia,
# MAGIC     e.tipo,
# MAGIC     e.origem,
# MAGIC     e.duracao_s,
# MAGIC     q.potencia_kw * q.fator_perda * e.duracao_s / 3600           AS kwh,
# MAGIC     e.custo_estimado_rs,
# MAGIC     t.adicional_bandeira_rs_kwh,
# MAGIC     e.duracao_s > 60                                             AS acima_limite_alerta,
# MAGIC     CASE WHEN to_date(e.inicio_local) >= l.data_inicio_alertas THEN 'depois' ELSE 'antes' END AS fase_alertas,
# MAGIC     to_date(e.inicio_local)                                      AS data
# MAGIC   FROM prata_eventos e
# MAGIC   JOIN dim_loja l        ON l.loja_id = e.loja_id
# MAGIC   JOIN dim_equipamento q ON q.loja_id = e.loja_id AND q.equipamento_id = e.equipamento_id
# MAGIC   JOIN dim_tempo t       ON t.data = to_date(e.inicio_local)
# MAGIC )
# MAGIC SELECT
# MAGIC   b.evento_id,
# MAGIC   b.loja_sk,
# MAGIC   b.equipamento_sk,
# MAGIC   b.data_id,
# MAGIC   b.hora,
# MAGIC   b.hora_referencia,
# MAGIC   b.tipo,
# MAGIC   b.origem,
# MAGIC   b.duracao_s,
# MAGIC   round(b.kwh, 6)                                                              AS kwh_desperdicado,
# MAGIC   b.custo_estimado_rs,
# MAGIC   round(b.kwh * (tf.tarifa_rs_kwh + b.adicional_bandeira_rs_kwh), 6)           AS custo_aneel_rs,
# MAGIC   c.temperatura_c,
# MAGIC   b.acima_limite_alerta,
# MAGIC   b.fase_alertas
# MAGIC FROM base b
# MAGIC LEFT JOIN tarifa tf    ON b.data BETWEEN tf.inicio_vigencia AND tf.fim_vigencia
# MAGIC LEFT JOIN prata_clima c ON c.data_hora = b.hora_referencia;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conferência: a fato tem exatamente uma linha por evento da Prata

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT count(*) FROM prata_eventos)                    AS eventos_na_prata,
# MAGIC   (SELECT count(*) FROM fato_desperdicio)                 AS linhas_na_fato,
# MAGIC   (SELECT count(DISTINCT evento_id) FROM fato_desperdicio) AS eventos_distintos_na_fato,
# MAGIC   (SELECT count(*) FROM fato_desperdicio WHERE custo_aneel_rs IS NULL) AS sem_tarifa_aneel,
# MAGIC   (SELECT count(*) FROM fato_desperdicio WHERE temperatura_c IS NULL)  AS sem_temperatura;

# COMMAND ----------

# MAGIC %md
# MAGIC ## vw_painel_indicadores — o contrato com o MVP
# MAGIC Desperdício por loja, equipamento e dia. O notebook 05 exporta esta visão em CSV, e o painel carrega o arquivo
# MAGIC na tela de indicadores. Não há dado pessoal nesta visão.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW vw_painel_indicadores AS
# MAGIC SELECT
# MAGIC   l.loja_id,
# MAGIC   q.equipamento_id,
# MAGIC   q.categoria,
# MAGIC   t.data,
# MAGIC   t.periodo,
# MAGIC   f.tipo,
# MAGIC   count(*)                                   AS eventos,
# MAGIC   count_if(f.acima_limite_alerta)            AS eventos_acima_do_limite,
# MAGIC   sum(f.duracao_s)                           AS duracao_total_s,
# MAGIC   round(avg(f.duracao_s), 1)                 AS duracao_media_s,
# MAGIC   round(sum(f.kwh_desperdicado), 4)          AS kwh_desperdicado,
# MAGIC   round(sum(f.custo_estimado_rs), 4)         AS custo_estimado_rs,
# MAGIC   round(sum(f.custo_aneel_rs), 4)            AS custo_aneel_rs,
# MAGIC   t.bandeira,
# MAGIC   t.temp_max_c
# MAGIC FROM fato_desperdicio f
# MAGIC JOIN dim_loja l        ON l.loja_sk = f.loja_sk
# MAGIC JOIN dim_equipamento q ON q.equipamento_sk = f.equipamento_sk
# MAGIC JOIN dim_tempo t       ON t.data_id = f.data_id
# MAGIC GROUP BY ALL;
# MAGIC
# MAGIC SELECT * FROM vw_painel_indicadores ORDER BY data DESC, loja_id, equipamento_id LIMIT 30;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidência de LGPD (requisito 08)
# MAGIC Lado a lado: a Prata (acesso restrito) com o dado em claro e a Ouro com hash e máscara.
# MAGIC Os dados do cadastro são fictícios.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   p.loja_id,
# MAGIC   p.responsavel_nome      AS prata_nome,
# MAGIC   o.responsavel_nome_mascarado     AS ouro_nome,
# MAGIC   p.responsavel_email     AS prata_email,
# MAGIC   o.responsavel_email_mascarado    AS ouro_email,
# MAGIC   p.responsavel_telefone  AS prata_telefone,
# MAGIC   o.responsavel_telefone_mascarado AS ouro_telefone,
# MAGIC   left(o.responsavel_hash, 16) || '...' AS ouro_hash
# MAGIC FROM prata_lojas p
# MAGIC JOIN dim_loja o ON o.loja_id = p.loja_id
# MAGIC ORDER BY p.loja_id;

-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · As 5 consultas do MVP (requisito 07)
-- MAGIC Cada consulta responde a uma pergunta que o comerciante faz ao painel. A 5ª é a de **impacto**.
-- MAGIC
-- MAGIC Os eventos das lojas são simulados (ver `dags/simulador_eventos.py`). As consultas mostram **como** cada pergunta
-- MAGIC é respondida; os valores refletem as hipóteses do simulador até que entrem eventos reais (ACV e Maker Lab).

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA energia_lucro;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 1 — Quanto cada loja desperdiçou no mês, em R$ e em kWh?
-- MAGIC Alimenta o número principal da tela "desperdício" do painel.

-- COMMAND ----------

SELECT
  l.loja_id,
  t.periodo,
  count(*)                               AS eventos,
  count(DISTINCT t.data)                 AS dias_com_dados,
  round(sum(f.kwh_desperdicado), 2)      AS kwh_desperdicado,
  round(sum(f.custo_estimado_rs), 2)     AS custo_estimado_rs,
  round(sum(f.custo_aneel_rs), 2)        AS custo_aneel_rs,
  round(sum(f.custo_estimado_rs) / count(DISTINCT t.data) * 30, 2) AS projecao_rs_30_dias
FROM fato_desperdicio f
JOIN dim_loja l  ON l.loja_sk = f.loja_sk
JOIN dim_tempo t ON t.data_id = f.data_id
GROUP BY l.loja_id, t.periodo
ORDER BY t.periodo, custo_estimado_rs DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 2 — Quais equipamentos mais desperdiçam?
-- MAGIC Mostra onde agir primeiro: participação no custo da loja, tempo médio e parcela dos eventos que passam do limite de 60 s.

-- COMMAND ----------

SELECT
  l.loja_id,
  q.equipamento_id,
  q.categoria,
  count(*)                                                        AS eventos,
  round(avg(f.duracao_s), 0)                                      AS duracao_media_s,
  round(100 * avg(CASE WHEN f.acima_limite_alerta THEN 1 ELSE 0 END), 1) AS pct_acima_de_60s,
  round(sum(f.kwh_desperdicado), 2)                               AS kwh_desperdicado,
  round(sum(f.custo_estimado_rs), 2)                              AS custo_estimado_rs,
  round(100 * sum(f.custo_estimado_rs) / sum(sum(f.custo_estimado_rs)) OVER (PARTITION BY l.loja_id), 1) AS pct_do_custo_da_loja
FROM fato_desperdicio f
JOIN dim_loja l        ON l.loja_sk = f.loja_sk
JOIN dim_equipamento q ON q.equipamento_sk = f.equipamento_sk
GROUP BY l.loja_id, q.equipamento_id, q.categoria
ORDER BY l.loja_id, custo_estimado_rs DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 3 — Em que horários o desperdício se concentra?
-- MAGIC Indica quando reforçar a atenção da equipe (e quando o alerta precisa chegar mais rápido).

-- COMMAND ----------

SELECT
  f.hora,
  count(*)                                                            AS eventos,
  count_if(f.tipo = 'porta_aberta')                                   AS portas_abertas,
  count_if(f.tipo = 'luz_acesa_sem_necessidade')                      AS luzes_acesas,
  round(sum(f.kwh_desperdicado), 2)                                   AS kwh_desperdicado,
  round(sum(f.custo_estimado_rs), 2)                                  AS custo_estimado_rs,
  round(100 * sum(f.custo_estimado_rs) / sum(sum(f.custo_estimado_rs)) OVER (), 1) AS pct_do_custo_total
FROM fato_desperdicio f
GROUP BY f.hora
ORDER BY f.hora;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 4 — Dias e horas mais quentes têm mais desperdício na refrigeração?
-- MAGIC Cruza a temperatura real (Open-Meteo) com as portas abertas, nas horas em que as duas lojas estão abertas (7h às 19h).
-- MAGIC `eventos_por_hora` normaliza pelo número de horas observadas em cada faixa, para faixas raras não parecerem melhores.
-- MAGIC
-- MAGIC Atenção na interpretação: no simulador, as aberturas seguem um perfil horário com pico às 14h, que também é a hora
-- MAGIC mais quente. A relação que aparecer aqui é a hipótese a validar com eventos reais, não uma medição.

-- COMMAND ----------

WITH horas AS (
  SELECT
    data_hora,
    temperatura_c,
    CASE
      WHEN temperatura_c < 18 THEN '1) abaixo de 18 °C'
      WHEN temperatura_c < 22 THEN '2) 18 a 21,9 °C'
      WHEN temperatura_c < 26 THEN '3) 22 a 25,9 °C'
      WHEN temperatura_c < 30 THEN '4) 26 a 29,9 °C'
      ELSE '5) 30 °C ou mais'
    END AS faixa_temperatura
  FROM prata_clima
  WHERE hour(data_hora) BETWEEN 7 AND 19
    AND to_date(data_hora) IN (SELECT data FROM dim_tempo)
)
SELECT
  h.faixa_temperatura,
  count(DISTINCT h.data_hora)                                   AS horas_observadas,
  count(f.evento_id)                                            AS portas_abertas,
  round(count(f.evento_id) / count(DISTINCT h.data_hora), 2)    AS eventos_por_hora,
  round(avg(f.duracao_s), 0)                                    AS duracao_media_s,
  round(sum(f.kwh_desperdicado) / count(DISTINCT h.data_hora), 4) AS kwh_por_hora
FROM horas h
LEFT JOIN fato_desperdicio f
  ON f.hora_referencia = h.data_hora AND f.tipo = 'porta_aberta'
GROUP BY h.faixa_temperatura
ORDER BY h.faixa_temperatura;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 5 (impacto) — Quanto a loja deixou de desperdiçar depois que os alertas começaram?
-- MAGIC Compara a média diária antes e depois de `data_inicio_alertas` de cada loja e projeta a economia para 30 dias.
-- MAGIC É o indicador de impacto da Ficha Técnica: **R$ e kWh evitados por loja ao mês** e **tempo médio de porta aberta**.
-- MAGIC
-- MAGIC Atenção na interpretação: com dados simulados, a redução vem da hipótese "70% dos alertas são atendidos em até 60 s".
-- MAGIC A consulta é a régua que vai medir o impacto real quando houver lojas piloto.

-- COMMAND ----------

WITH diario AS (
  SELECT
    l.loja_id,
    t.data,
    f.fase_alertas,
    sum(f.custo_estimado_rs)                                        AS rs_dia,
    sum(f.custo_aneel_rs)                                           AS rs_aneel_dia,
    sum(f.kwh_desperdicado)                                         AS kwh_dia,
    avg(CASE WHEN f.tipo = 'porta_aberta' THEN f.duracao_s END)     AS porta_media_s,
    avg(CASE WHEN f.tipo = 'porta_aberta' AND f.duracao_s <= 120 THEN 1.0
             WHEN f.tipo = 'porta_aberta' THEN 0.0 END)             AS pct_portas_ate_2min
  FROM fato_desperdicio f
  JOIN dim_loja l  ON l.loja_sk = f.loja_sk
  JOIN dim_tempo t ON t.data_id = f.data_id
  GROUP BY l.loja_id, t.data, f.fase_alertas
)
SELECT
  loja_id,
  count_if(fase_alertas = 'antes')                                             AS dias_antes,
  count_if(fase_alertas = 'depois')                                            AS dias_depois,
  round(avg(CASE WHEN fase_alertas = 'antes'  THEN rs_dia END), 4)             AS rs_dia_antes,
  round(avg(CASE WHEN fase_alertas = 'depois' THEN rs_dia END), 4)             AS rs_dia_depois,
  round(100 * (1 - avg(CASE WHEN fase_alertas = 'depois' THEN rs_dia END)
                 / avg(CASE WHEN fase_alertas = 'antes'  THEN rs_dia END)), 1) AS reducao_pct,
  round(30 * (avg(CASE WHEN fase_alertas = 'antes'  THEN rs_dia END)
            - avg(CASE WHEN fase_alertas = 'depois' THEN rs_dia END)), 2)      AS rs_evitados_em_30_dias,
  round(30 * (avg(CASE WHEN fase_alertas = 'antes'  THEN rs_aneel_dia END)
            - avg(CASE WHEN fase_alertas = 'depois' THEN rs_aneel_dia END)), 2) AS rs_aneel_evitados_em_30_dias,
  round(30 * (avg(CASE WHEN fase_alertas = 'antes'  THEN kwh_dia END)
            - avg(CASE WHEN fase_alertas = 'depois' THEN kwh_dia END)), 2)     AS kwh_evitados_em_30_dias,
  round(avg(CASE WHEN fase_alertas = 'antes'  THEN porta_media_s END), 0)      AS porta_media_s_antes,
  round(avg(CASE WHEN fase_alertas = 'depois' THEN porta_media_s END), 0)      AS porta_media_s_depois,
  round(100 * avg(CASE WHEN fase_alertas = 'antes'  THEN pct_portas_ate_2min END), 1) AS pct_portas_ate_2min_antes,
  round(100 * avg(CASE WHEN fase_alertas = 'depois' THEN pct_portas_ate_2min END), 1) AS pct_portas_ate_2min_depois
FROM diario
GROUP BY loja_id
ORDER BY loja_id;

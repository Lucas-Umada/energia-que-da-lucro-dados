-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 · Prata — dado tratado (requisito 06)
-- MAGIC
-- MAGIC Para cada fonte: **tipos corretos**, **nulos e inválidos tratados**, **regras de qualidade com o total barrado**
-- MAGIC e **MERGE pela chave** (rodar de novo não duplica).
-- MAGIC
-- MAGIC | Tabela | Chave do MERGE | Regras de qualidade |
-- MAGIC |---|---|---|
-- MAGIC | `prata_eventos` | `id` | R0 id nulo · R1 duração fora de 1 s a 4 h · R2 loja/equipamento fora do cadastro · R3 início inválido ou no futuro · R4 tipo/origem fora do contrato · D1 id repetido |
-- MAGIC | `prata_clima` | `cidade, data_hora` | C1 hora inválida · C2 temperatura nula ou fora de −10 a 50 °C |
-- MAGIC | `prata_bandeiras` | `competencia` | B1 competência inválida · B2 adicional nulo ou negativo |
-- MAGIC | `prata_tarifas` | `distribuidora, subgrupo, modalidade, inicio_vigencia` | T1 vigência inválida · T2 tarifa nula ou não positiva |
-- MAGIC
-- MAGIC Os registros barrados de eventos ficam em `prata_eventos_rejeitados`, com o motivo. Os totais ficam em `prata_qualidade_log`.

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA energia_lucro;

CREATE TABLE IF NOT EXISTS prata_qualidade_log (
  executado_em TIMESTAMP, tabela STRING, regra STRING, descricao STRING,
  total_avaliado BIGINT, total_barrado BIGINT
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cadastro (lojas e equipamentos)
-- MAGIC Tabelas pequenas de referência: são refeitas a cada execução, ficando com a versão mais recente de cada chave.
-- MAGIC `prata_lojas` ainda tem os dados pessoais em claro; o acesso a ela deve ser restrito. A proteção é aplicada na Ouro.

-- COMMAND ----------

CREATE OR REPLACE TABLE prata_lojas AS
SELECT
  trim(loja_id)                            AS loja_id,
  trim(nome_fantasia)                      AS nome_fantasia,
  lower(trim(tipo))                        AS tipo,
  trim(cidade)                             AS cidade,
  upper(trim(uf))                          AS uf,
  try_cast(hora_abre AS INT)               AS hora_abre,
  try_cast(hora_fecha AS INT)              AS hora_fecha,
  trim(responsavel_nome)                   AS responsavel_nome,
  lower(trim(responsavel_email))           AS responsavel_email,
  trim(responsavel_telefone)               AS responsavel_telefone,
  try_cast(data_inicio_alertas AS DATE)    AS data_inicio_alertas,
  carregado_em
FROM bronze_cadastro_lojas
WHERE nullif(trim(loja_id), '') IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY trim(loja_id) ORDER BY carregado_em DESC) = 1;

CREATE OR REPLACE TABLE prata_equipamentos AS
SELECT
  trim(loja_id)                      AS loja_id,
  trim(equipamento_id)               AS equipamento_id,
  lower(trim(categoria))             AS categoria,
  try_cast(potencia_kw AS DOUBLE)    AS potencia_kw,
  try_cast(fator_perda AS DOUBLE)    AS fator_perda,
  carregado_em
FROM bronze_cadastro_equipamentos
WHERE nullif(trim(loja_id), '') IS NOT NULL
  AND nullif(trim(equipamento_id), '') IS NOT NULL
  AND try_cast(potencia_kw AS DOUBLE) > 0
QUALIFY row_number() OVER (PARTITION BY trim(loja_id), trim(equipamento_id) ORDER BY carregado_em DESC) = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Eventos de desperdício
-- MAGIC ### 1. Tipagem
-- MAGIC `inicio_ts` é o instante real (usado na regra "não futuro"). `inicio_local` é a hora de relógio em Brasília,
-- MAGIC usada para extrair dia e hora sem depender do fuso da sessão.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW eventos_tipados AS
SELECT
  nullif(trim(id), '')                                         AS id,
  nullif(trim(loja_id), '')                                    AS loja_id,
  nullif(trim(equipamento_id), '')                             AS equipamento_id,
  lower(trim(tipo))                                            AS tipo,
  lower(trim(origem))                                          AS origem,
  inicio                                                       AS inicio_texto,
  try_to_timestamp(inicio)                                     AS inicio_ts,
  try_to_timestamp(replace(substring(inicio, 1, 19), 'T', ' ')) AS inicio_local,
  try_cast(duracao_s AS INT)                                   AS duracao_s,
  try_cast(custo_estimado_rs AS DOUBLE)                        AS custo_estimado_rs,
  arquivo_origem,
  carregado_em
FROM bronze_eventos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Regras de qualidade
-- MAGIC Cada regra vira uma coluna booleana, para contar o total barrado por regra (um registro pode falhar em mais de uma).

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW eventos_avaliados AS
SELECT
  e.*,
  q.potencia_kw,
  q.fator_perda,
  e.id IS NULL                                                              AS falha_r0_id,
  e.duracao_s IS NULL OR e.duracao_s <= 0 OR e.duracao_s > 14400            AS falha_r1_duracao,
  q.equipamento_id IS NULL                                                  AS falha_r2_cadastro,
  e.inicio_ts IS NULL OR e.inicio_local IS NULL OR e.inicio_ts > e.carregado_em AS falha_r3_inicio,
  NOT coalesce(e.tipo IN ('porta_aberta', 'luz_acesa_sem_necessidade', 'temperatura_alta'), false)
    OR NOT coalesce(e.origem IN ('acv', 'maker', 'simulador'), false)       AS falha_r4_dominio
FROM eventos_tipados e
LEFT JOIN prata_equipamentos q
  ON q.loja_id = e.loja_id AND q.equipamento_id = e.equipamento_id;

CREATE OR REPLACE TEMP VIEW eventos_validos AS
SELECT * FROM eventos_avaliados
WHERE NOT (falha_r0_id OR falha_r1_duracao OR falha_r2_cadastro OR falha_r3_inicio OR falha_r4_dominio);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Registros barrados, com o motivo

-- COMMAND ----------

CREATE OR REPLACE TABLE prata_eventos_rejeitados AS
SELECT
  id, loja_id, equipamento_id, tipo, origem, inicio_texto, duracao_s, arquivo_origem, carregado_em,
  filter(array(
    CASE WHEN falha_r0_id       THEN 'R0_id_nulo' END,
    CASE WHEN falha_r1_duracao  THEN 'R1_duracao_invalida' END,
    CASE WHEN falha_r2_cadastro THEN 'R2_fora_do_cadastro' END,
    CASE WHEN falha_r3_inicio   THEN 'R3_inicio_invalido_ou_futuro' END,
    CASE WHEN falha_r4_dominio  THEN 'R4_tipo_ou_origem_fora_do_contrato' END
  ), m -> m IS NOT NULL) AS motivos
FROM eventos_avaliados
WHERE falha_r0_id OR falha_r1_duracao OR falha_r2_cadastro OR falha_r3_inicio OR falha_r4_dominio;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Lote limpo, sem id repetido, e MERGE pelo `id`
-- MAGIC Custo nulo é recalculado pela fórmula do contrato (tarifa provisória de R$ 0,90/kWh).

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW lote_prata_eventos AS
SELECT
  id, loja_id, equipamento_id, tipo, origem, inicio_ts, inicio_local, duracao_s,
  coalesce(custo_estimado_rs, round(potencia_kw * fator_perda * duracao_s / 3600 * 0.90, 4)) AS custo_estimado_rs,
  arquivo_origem,
  carregado_em
FROM eventos_validos
QUALIFY row_number() OVER (PARTITION BY id ORDER BY carregado_em DESC, arquivo_origem DESC) = 1;

CREATE TABLE IF NOT EXISTS prata_eventos (
  id STRING NOT NULL,
  loja_id STRING,
  equipamento_id STRING,
  tipo STRING,
  origem STRING,
  inicio_ts TIMESTAMP,
  inicio_local TIMESTAMP,
  duracao_s INT,
  custo_estimado_rs DOUBLE,
  arquivo_origem STRING,
  carregado_em TIMESTAMP
);

MERGE INTO prata_eventos AS alvo
USING lote_prata_eventos AS novo
ON alvo.id = novo.id
WHEN MATCHED AND novo.carregado_em > alvo.carregado_em THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

INSERT INTO prata_qualidade_log
SELECT current_timestamp(), 'prata_eventos', 'R0', 'id nulo ou vazio', count(*), count_if(falha_r0_id) FROM eventos_avaliados
UNION ALL
SELECT current_timestamp(), 'prata_eventos', 'R1', 'duracao_s nula, menor ou igual a zero ou acima de 4 horas', count(*), count_if(falha_r1_duracao) FROM eventos_avaliados
UNION ALL
SELECT current_timestamp(), 'prata_eventos', 'R2', 'loja_id ou equipamento_id fora do cadastro', count(*), count_if(falha_r2_cadastro) FROM eventos_avaliados
UNION ALL
SELECT current_timestamp(), 'prata_eventos', 'R3', 'inicio invalido ou no futuro', count(*), count_if(falha_r3_inicio) FROM eventos_avaliados
UNION ALL
SELECT current_timestamp(), 'prata_eventos', 'R4', 'tipo ou origem fora do contrato de dados', count(*), count_if(falha_r4_dominio) FROM eventos_avaliados
UNION ALL
SELECT current_timestamp(), 'prata_eventos', 'D1', 'id repetido entre os registros validos (mantida a carga mais recente)', count(*), count(*) - count(DISTINCT id) FROM eventos_validos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clima (Open-Meteo)
-- MAGIC O JSON traz listas paralelas (`time`, `temperature_2m`, ...). `posexplode` abre uma linha por hora.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW clima_tipado AS
SELECT
  'sao-paulo'                                              AS cidade,
  try_to_timestamp(replace(h.hora_texto, 'T', ' '))        AS data_hora,
  try_element_at(b.hourly.temperature_2m, h.pos + 1)       AS temperatura_c,
  try_element_at(b.hourly.relative_humidity_2m, h.pos + 1) AS umidade_pct,
  b.arquivo_origem,
  b.carregado_em
FROM bronze_clima b
LATERAL VIEW posexplode(b.hourly.time) h AS pos, hora_texto;

CREATE OR REPLACE TEMP VIEW clima_avaliado AS
SELECT *,
  data_hora IS NULL                                                       AS falha_c1_hora,
  temperatura_c IS NULL OR temperatura_c < -10 OR temperatura_c > 50      AS falha_c2_temperatura
FROM clima_tipado;

CREATE OR REPLACE TEMP VIEW lote_prata_clima AS
SELECT cidade, data_hora, temperatura_c, umidade_pct, arquivo_origem, carregado_em
FROM clima_avaliado
WHERE NOT (falha_c1_hora OR falha_c2_temperatura)
QUALIFY row_number() OVER (PARTITION BY cidade, data_hora ORDER BY carregado_em DESC, arquivo_origem DESC) = 1;

CREATE TABLE IF NOT EXISTS prata_clima (
  cidade STRING, data_hora TIMESTAMP, temperatura_c DOUBLE, umidade_pct DOUBLE,
  arquivo_origem STRING, carregado_em TIMESTAMP
);

MERGE INTO prata_clima AS alvo
USING lote_prata_clima AS novo
ON alvo.cidade = novo.cidade AND alvo.data_hora = novo.data_hora
WHEN MATCHED AND novo.carregado_em > alvo.carregado_em THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

INSERT INTO prata_qualidade_log
SELECT current_timestamp(), 'prata_clima', 'C1', 'data_hora invalida', count(*), count_if(falha_c1_hora) FROM clima_avaliado
UNION ALL
SELECT current_timestamp(), 'prata_clima', 'C2', 'temperatura nula ou fora de -10 a 50 graus', count(*), count_if(falha_c2_temperatura) FROM clima_avaliado;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bandeiras tarifárias (ANEEL)
-- MAGIC Cada arquivo diário traz o histórico inteiro; o MERGE pela competência mantém uma linha por mês.
-- MAGIC O valor vem com vírgula decimal (`"18,85"`, em R$/MWh).

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW bandeiras_avaliadas AS
SELECT *,
  competencia IS NULL                                 AS falha_b1_competencia,
  adicional_rs_mwh IS NULL OR adicional_rs_mwh < 0    AS falha_b2_adicional
FROM (
  SELECT
    try_cast(DatCompetencia AS DATE)                                                     AS competencia,
    initcap(trim(NomBandeiraAcionada))                                                   AS bandeira,
    try_cast(replace(replace(VlrAdicionalBandeira, '.', ''), ',', '.') AS DOUBLE)        AS adicional_rs_mwh,
    try_cast(DatGeracaoConjuntoDados AS DATE)                                            AS gerado_em,
    arquivo_origem,
    carregado_em
  FROM bronze_bandeiras
);

CREATE OR REPLACE TEMP VIEW lote_prata_bandeiras AS
SELECT competencia, bandeira, adicional_rs_mwh, gerado_em, arquivo_origem, carregado_em
FROM bandeiras_avaliadas
WHERE NOT (falha_b1_competencia OR falha_b2_adicional)
QUALIFY row_number() OVER (PARTITION BY competencia ORDER BY gerado_em DESC, carregado_em DESC, arquivo_origem DESC) = 1;

CREATE TABLE IF NOT EXISTS prata_bandeiras (
  competencia DATE, bandeira STRING, adicional_rs_mwh DOUBLE, gerado_em DATE,
  arquivo_origem STRING, carregado_em TIMESTAMP
);

MERGE INTO prata_bandeiras AS alvo
USING lote_prata_bandeiras AS novo
ON alvo.competencia = novo.competencia
WHEN MATCHED AND novo.carregado_em > alvo.carregado_em THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

INSERT INTO prata_qualidade_log
SELECT current_timestamp(), 'prata_bandeiras', 'B1', 'competencia invalida', count(*), count_if(falha_b1_competencia) FROM bandeiras_avaliadas
UNION ALL
SELECT current_timestamp(), 'prata_bandeiras', 'B2', 'adicional nulo ou negativo', count(*), count_if(falha_b2_adicional) FROM bandeiras_avaliadas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tarifas de aplicação B3 (ANEEL)
-- MAGIC Fica só a tarifa cheia (`DscDetalhe = 'Não se aplica'`; a linha `SCEE` é de geração distribuída).
-- MAGIC `tarifa_rs_kwh = (TUSD + TE) / 1000`, **sem tributos** (ICMS e PIS/COFINS não entram na tarifa homologada).

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW tarifas_avaliadas AS
SELECT *,
  inicio_vigencia IS NULL OR fim_vigencia IS NULL OR fim_vigencia < inicio_vigencia  AS falha_t1_vigencia,
  tusd_rs_mwh IS NULL OR te_rs_mwh IS NULL OR tusd_rs_mwh + te_rs_mwh <= 0           AS falha_t2_valor
FROM (
  SELECT
    trim(SigAgente)                                                          AS distribuidora,
    trim(DscSubGrupo)                                                        AS subgrupo,
    trim(DscModalidadeTarifaria)                                             AS modalidade,
    try_cast(DatInicioVigencia AS DATE)                                      AS inicio_vigencia,
    try_cast(DatFimVigencia AS DATE)                                         AS fim_vigencia,
    try_cast(replace(replace(VlrTUSD, '.', ''), ',', '.') AS DOUBLE)         AS tusd_rs_mwh,
    try_cast(replace(replace(VlrTE, '.', ''), ',', '.') AS DOUBLE)           AS te_rs_mwh,
    trim(DscREH)                                                             AS resolucao,
    arquivo_origem,
    carregado_em
  FROM bronze_tarifas
  WHERE trim(DscDetalhe) = 'Não se aplica'
    AND trim(NomPostoTarifario) = 'Não se aplica'
);

CREATE OR REPLACE TEMP VIEW lote_prata_tarifas AS
SELECT
  distribuidora, subgrupo, modalidade, inicio_vigencia, fim_vigencia, tusd_rs_mwh, te_rs_mwh,
  round((tusd_rs_mwh + te_rs_mwh) / 1000, 5) AS tarifa_rs_kwh,
  resolucao, arquivo_origem, carregado_em
FROM tarifas_avaliadas
WHERE NOT (falha_t1_vigencia OR falha_t2_valor)
QUALIFY row_number() OVER (
  PARTITION BY distribuidora, subgrupo, modalidade, inicio_vigencia
  ORDER BY carregado_em DESC, arquivo_origem DESC
) = 1;

CREATE TABLE IF NOT EXISTS prata_tarifas (
  distribuidora STRING, subgrupo STRING, modalidade STRING, inicio_vigencia DATE, fim_vigencia DATE,
  tusd_rs_mwh DOUBLE, te_rs_mwh DOUBLE, tarifa_rs_kwh DOUBLE, resolucao STRING,
  arquivo_origem STRING, carregado_em TIMESTAMP
);

MERGE INTO prata_tarifas AS alvo
USING lote_prata_tarifas AS novo
ON  alvo.distribuidora = novo.distribuidora
AND alvo.subgrupo = novo.subgrupo
AND alvo.modalidade = novo.modalidade
AND alvo.inicio_vigencia = novo.inicio_vigencia
WHEN MATCHED AND novo.carregado_em > alvo.carregado_em THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

INSERT INTO prata_qualidade_log
SELECT current_timestamp(), 'prata_tarifas', 'T1', 'vigencia invalida', count(*), count_if(falha_t1_vigencia) FROM tarifas_avaliadas
UNION ALL
SELECT current_timestamp(), 'prata_tarifas', 'T2', 'tarifa nula ou nao positiva', count(*), count_if(falha_t2_valor) FROM tarifas_avaliadas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Evidências para o relatório
-- MAGIC ### Total barrado por regra (execução mais recente)

-- COMMAND ----------

SELECT tabela, regra, descricao, total_avaliado, total_barrado, executado_em
FROM prata_qualidade_log
QUALIFY row_number() OVER (PARTITION BY tabela, regra ORDER BY executado_em DESC) = 1
ORDER BY tabela, regra;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Contagem das tabelas da Prata
-- MAGIC Rodar este notebook duas vezes seguidas deve manter estes números iguais (MERGE idempotente).

-- COMMAND ----------

SELECT 'prata_eventos' AS tabela, count(*) AS linhas, count(DISTINCT id) AS chaves_distintas FROM prata_eventos
UNION ALL SELECT 'prata_eventos_rejeitados', count(*), NULL FROM prata_eventos_rejeitados
UNION ALL SELECT 'prata_clima', count(*), count(DISTINCT cidade, data_hora) FROM prata_clima
UNION ALL SELECT 'prata_bandeiras', count(*), count(DISTINCT competencia) FROM prata_bandeiras
UNION ALL SELECT 'prata_tarifas', count(*), count(DISTINCT distribuidora, subgrupo, modalidade, inicio_vigencia) FROM prata_tarifas
UNION ALL SELECT 'prata_lojas', count(*), count(DISTINCT loja_id) FROM prata_lojas
UNION ALL SELECT 'prata_equipamentos', count(*), count(DISTINCT loja_id, equipamento_id) FROM prata_equipamentos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exemplos de registros barrados

-- COMMAND ----------

SELECT motivos, id, loja_id, equipamento_id, inicio_texto, duracao_s, regexp_extract(arquivo_origem, '[^/]+$', 0) AS arquivo
FROM prata_eventos_rejeitados
ORDER BY carregado_em DESC
LIMIT 20;

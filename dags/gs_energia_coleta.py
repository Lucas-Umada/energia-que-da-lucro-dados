"""DAG de coleta diária do componente Dados - Energia que dá Lucro (GS 2026.2).

Cada execução coleta UM dia fechado (o dia anterior, no horário de Brasília) e
grava um arquivo por fonte na pasta de pouso, com a data no nome:

    chegada/clima/clima_AAAA-MM-DD.json           Open-Meteo (API real, sem chave)
    chegada/bandeiras/bandeiras_AAAA-MM-DD.json   ANEEL dados abertos (real)
    chegada/tarifas/tarifas_AAAA-MM-DD.json       ANEEL dados abertos (real)
    chegada/eventos/eventos_AAAA-MM-DD.json       eventos das lojas (simulados)

As quatro coletas rodam em paralelo; `conferir_chegada` valida os arquivos no fim.

Configuração por Airflow Variables (Admin > Variables). Nenhuma das fontes exige
chave de API; se uma fonte com chave for adicionada, a chave entra aqui também,
nunca no código:

    gs_chegada_dir              pasta de pouso (padrão: ../chegada ao lado de dags/)
    gs_latitude / gs_longitude  coordenadas das lojas (padrão: São Paulo)
    gs_distribuidora            sigla da distribuidora na ANEEL (padrão: ELETROPAULO)
    gs_aneel_recurso_bandeiras  id do recurso CKAN "Bandeira Tarifária - Acionamento"
    gs_aneel_recurso_tarifas    id do recurso CKAN "Tarifas homologadas"

Escrito para Airflow 3 (imports de `airflow.sdk` e `airflow.providers.standard`).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pendulum
import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

import simulador_eventos

log = logging.getLogger(__name__)

FUSO = "America/Sao_Paulo"
FONTES = ("clima", "bandeiras", "tarifas", "eventos")
ANEEL_API = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search"

PADROES = {
    "gs_chegada_dir": str(Path(__file__).resolve().parent.parent / "chegada"),
    "gs_latitude": "-23.55",
    "gs_longitude": "-46.63",
    "gs_distribuidora": "ELETROPAULO",
    "gs_aneel_recurso_bandeiras": "0591b8f6-fe54-437b-b72b-1aa2efd46e42",
    "gs_aneel_recurso_tarifas": "fcf2906c-7c32-4b9b-a637-054e7a5234f4",
}


def _var(nome: str) -> str:
    """Lê a Variable dentro da tarefa (não no parse do DAG)."""
    return Variable.get(nome, default=PADROES[nome])


def _dia_de_coleta(contexto: dict) -> str:
    """Dia fechado a coletar: a véspera da execução, no fuso de Brasília.

    No Airflow 3 o `logical_date` de um DAG com cron é o instante do disparo, e
    pode ser nulo em execuções manuais; por isso a data é calculada aqui em vez
    de usar `ds` direto.
    """
    referencia = contexto.get("logical_date")
    if referencia is None:
        dag_run = contexto.get("dag_run")
        referencia = getattr(dag_run, "run_after", None) or pendulum.now("UTC")
    return pendulum.instance(referencia).in_timezone(FUSO).subtract(days=1).to_date_string()


def _arquivo(fonte: str, dia: str) -> Path:
    return Path(_var("gs_chegada_dir")) / fonte / f"{fonte}_{dia}.json"


def _gravar(fonte: str, dia: str, texto: str) -> Path:
    """Grava em arquivo temporário e renomeia, para nunca deixar arquivo pela metade."""
    destino = _arquivo(fonte, dia)
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_suffix(".tmp")
    temporario.write_text(texto, encoding="utf-8")
    temporario.replace(destino)
    log.info("Gravado %s (%d bytes)", destino, destino.stat().st_size)
    return destino


def _jsonl(registros: list[dict]) -> str:
    return "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in registros)


def _aneel(recurso: str, **parametros) -> list[dict]:
    resposta = requests.get(ANEEL_API, params={"resource_id": recurso, **parametros}, timeout=60)
    resposta.raise_for_status()
    corpo = resposta.json()
    if not corpo.get("success"):
        raise RuntimeError(f"ANEEL respondeu success=false para o recurso {recurso}")
    registros = corpo["result"]["records"]
    # _id e rank são campos internos do CKAN, não do conjunto de dados.
    return [{k: v for k, v in r.items() if k not in ("_id", "rank")} for r in registros]


def coletar_clima(**contexto):
    """Temperatura e umidade por hora do dia coletado (Open-Meteo)."""
    dia = _dia_de_coleta(contexto)
    idade_dias = (pendulum.now(FUSO).date() - pendulum.parse(dia).date()).days
    # A API de previsão guarda cerca de 3 meses para trás; antes disso, o arquivo histórico.
    if idade_dias <= 80:
        url = "https://api.open-meteo.com/v1/forecast"
    else:
        url = "https://archive-api.open-meteo.com/v1/archive"
    resposta = requests.get(
        url,
        params={
            "latitude": _var("gs_latitude"),
            "longitude": _var("gs_longitude"),
            "start_date": dia,
            "end_date": dia,
            "hourly": "temperature_2m,relative_humidity_2m",
            "timezone": FUSO,
        },
        timeout=30,
    )
    resposta.raise_for_status()
    horas = resposta.json().get("hourly", {})
    if len(horas.get("time", [])) != 24 or all(t is None for t in horas.get("temperature_2m", [])):
        raise RuntimeError(f"Open-Meteo devolveu dados incompletos para {dia}")
    _gravar("clima", dia, resposta.text)


def coletar_bandeiras(**contexto):
    """Bandeira tarifária acionada por mês e o adicional em R$/MWh (ANEEL)."""
    dia = _dia_de_coleta(contexto)
    registros = _aneel(_var("gs_aneel_recurso_bandeiras"), limit=500, sort="DatCompetencia desc")
    if not registros:
        raise RuntimeError("ANEEL devolveu zero registros de bandeiras")
    _gravar("bandeiras", dia, _jsonl(registros))


def coletar_tarifas(**contexto):
    """Tarifa de aplicação B3 convencional (comercial, baixa tensão) da distribuidora (ANEEL)."""
    dia = _dia_de_coleta(contexto)
    filtros = {
        "SigAgente": _var("gs_distribuidora"),
        "DscSubGrupo": "B3",
        "DscModalidadeTarifaria": "Convencional",
        "DscBaseTarifaria": "Tarifa de Aplicação",
    }
    registros = _aneel(
        _var("gs_aneel_recurso_tarifas"),
        filters=json.dumps(filtros, ensure_ascii=False),
        limit=200,
        sort="DatInicioVigencia desc",
    )
    if not registros:
        raise RuntimeError(f"ANEEL devolveu zero tarifas para {filtros['SigAgente']}")
    _gravar("tarifas", dia, _jsonl(registros))


def coletar_eventos(**contexto):
    """Eventos de desperdício das lojas. Simulados: ver dags/simulador_eventos.py."""
    dia = _dia_de_coleta(contexto)
    _gravar("eventos", dia, _jsonl(simulador_eventos.gerar_eventos(dia)))


def conferir_chegada(**contexto):
    """Confere se os quatro arquivos do dia chegaram, não estão vazios e são JSON válido."""
    dia = _dia_de_coleta(contexto)
    problemas, resumo = [], {}
    for fonte in FONTES:
        arquivo = _arquivo(fonte, dia)
        if not arquivo.exists():
            problemas.append(f"{fonte}: arquivo ausente ({arquivo})")
            continue
        texto = arquivo.read_text(encoding="utf-8")
        if not texto.strip():
            problemas.append(f"{fonte}: arquivo vazio")
            continue
        try:
            if fonte == "clima":
                registros = len(json.loads(texto)["hourly"]["time"])
            else:
                registros = sum(1 for linha in texto.splitlines() if linha.strip() and json.loads(linha))
        except (ValueError, KeyError) as erro:
            problemas.append(f"{fonte}: conteúdo inválido ({erro})")
            continue
        if registros == 0:
            problemas.append(f"{fonte}: zero registros")
        resumo[fonte] = registros
    log.info("Chegada de %s: %s", dia, resumo)
    if problemas:
        raise RuntimeError(f"Chegada incompleta em {dia}: " + "; ".join(problemas))
    return resumo


def notificar_falha(contexto):
    """Notificação de falha: fica registrada no log da tarefa e do scheduler."""
    tarefa = contexto.get("task_instance")
    log.error(
        "FALHA na coleta | dag=%s tarefa=%s tentativa=%s erro=%s",
        getattr(tarefa, "dag_id", "?"),
        getattr(tarefa, "task_id", "?"),
        getattr(tarefa, "try_number", "?"),
        contexto.get("exception"),
    )


with DAG(
    dag_id="gs_energia_coleta",
    description="Coleta diária: clima (Open-Meteo), bandeiras e tarifas (ANEEL) e eventos das lojas",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 11, 2, tz=FUSO),
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=2),
        "retry_exponential_backoff": True,
        "on_failure_callback": notificar_falha,
    },
    tags=["gs", "energia-que-da-lucro", "coleta"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")
    clima = PythonOperator(task_id="coletar_clima", python_callable=coletar_clima)
    bandeiras = PythonOperator(task_id="coletar_bandeiras", python_callable=coletar_bandeiras)
    tarifas = PythonOperator(task_id="coletar_tarifas", python_callable=coletar_tarifas)
    eventos = PythonOperator(task_id="coletar_eventos", python_callable=coletar_eventos)
    conferir = PythonOperator(task_id="conferir_chegada", python_callable=conferir_chegada)

    inicio >> [clima, bandeiras, tarifas, eventos] >> conferir

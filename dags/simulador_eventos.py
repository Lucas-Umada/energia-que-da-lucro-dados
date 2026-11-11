"""Simulador de eventos de desperdício das lojas (Energia que dá Lucro).

Por que simular: não existe API pública com o consumo de energia de um comércio
específico. O enunciado da GS permite "dados simulados com geração justificada e
documentada". Este módulo é essa documentação executável.

Formato de saída: o contrato de dados do grupo (seção 3 do guia), um evento por
linha (JSON Lines).

Hipóteses do simulador (todas são suposições do grupo, não medições):
  1. Aberturas de porta se concentram no meio da tarde (perfil horário com pico
     às 14h). Isso também é o horário mais quente do dia, então a relação
     "temperatura x desperdício" que aparece na camada Ouro vem deste perfil.
  2. A duração de uma porta aberta segue uma log-normal (muitas aberturas curtas,
     poucas muito longas).
  3. A partir de DATA_INICIO_ALERTAS o comerciante recebe alerta quando a porta
     passa de 60 s. Em 70% dos casos ele reage e a porta é fechada entre 15 e
     60 s depois do alerta. É essa hipótese que gera o "antes x depois" da
     consulta de impacto.
  4. Cerca de 3% dos registros saem defeituosos de propósito (duração inválida,
     loja inexistente, data futura, equipamento nulo, id repetido) para que as
     regras de qualidade da camada Prata tenham o que barrar.

A geração é determinística por dia: rodar de novo o mesmo dia produz o mesmo
arquivo, com os mesmos ids. Isso mantém o pipeline idempotente.
"""

from __future__ import annotations

import json
import math
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

VERSAO = "0.1"

# --- Parâmetros do contrato (seção 3 do guia; valores provisórios do grupo) ---
TARIFA_RS_KWH = 0.90
LIMITE_ALERTA_S = 60

# --- Hipóteses do simulador ---
DATA_INICIO_ALERTAS = date(2026, 11, 10)
PROB_ALERTA_RESOLVIDO = 0.70
TAXA_REGISTROS_DEFEITUOSOS = 0.03

# potencia_kw e fator_perda de refrigeração vêm do contrato do grupo.
# Os de iluminação NÃO estão no contrato: são suposição desta entrega
# (luz acesa sem necessidade = 100% de perda).
PERFIS = {
    "expositor": {
        "tipo": "porta_aberta",
        "potencia_kw": 0.50,
        "fator_perda": 0.30,
        "eventos_dia": (8, 16),
        "mediana_s": 75,
    },
    "freezer": {
        "tipo": "porta_aberta",
        "potencia_kw": 0.50,
        "fator_perda": 0.30,
        "eventos_dia": (3, 7),
        "mediana_s": 60,
    },
    "iluminacao-salao": {
        "tipo": "luz_acesa_sem_necessidade",
        "potencia_kw": 0.30,
        "fator_perda": 1.00,
    },
    "iluminacao-estoque": {
        "tipo": "luz_acesa_sem_necessidade",
        "potencia_kw": 0.10,
        "fator_perda": 1.00,
    },
}

LOJAS = {
    "padaria-01": {
        "abre": 6,
        "fecha": 20,
        "equipamentos": ["expositor-1", "expositor-2", "freezer-1", "iluminacao-salao"],
    },
    "mercado-01": {
        "abre": 7,
        "fecha": 21,
        "equipamentos": [
            "expositor-1",
            "expositor-2",
            "expositor-3",
            "freezer-1",
            "freezer-2",
            "iluminacao-salao",
            "iluminacao-estoque",
        ],
    },
}


def perfil_do_equipamento(equipamento_id: str) -> dict:
    """expositor-2 -> perfil 'expositor'; iluminacao-salao -> perfil próprio."""
    if equipamento_id in PERFIS:
        return PERFIS[equipamento_id]
    return PERFIS[equipamento_id.rsplit("-", 1)[0]]


def custo_estimado_rs(equipamento_id: str, duracao_s: int) -> float:
    """Fórmula do contrato: potencia_kw x fator_perda x (duracao_s/3600) x tarifa.

    Guardamos 4 casas decimais: com 2 casas quase todo evento viraria R$ 0,00.
    """
    perfil = perfil_do_equipamento(equipamento_id)
    custo = perfil["potencia_kw"] * perfil["fator_perda"] * (duracao_s / 3600) * TARIFA_RS_KWH
    return round(custo, 4)


def _peso_hora(hora: int) -> float:
    """Perfil horário: base 1 com pico às 14h (hipótese 1)."""
    return 1.0 + 2.0 * math.exp(-((hora - 14) ** 2) / (2 * 2.5**2))


def _uuid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _iso(momento: datetime) -> str:
    """ISO 8601 com o fuso de Brasília fixo, como pede o contrato."""
    return momento.strftime("%Y-%m-%dT%H:%M:%S") + "-03:00"


def _evento(rng, loja_id, equipamento_id, inicio: datetime, duracao_s: int) -> dict:
    return {
        "id": _uuid(rng),
        "loja_id": loja_id,
        "equipamento_id": equipamento_id,
        "tipo": perfil_do_equipamento(equipamento_id)["tipo"],
        "origem": "simulador",
        "inicio": _iso(inicio),
        "duracao_s": duracao_s,
        "custo_estimado_rs": custo_estimado_rs(equipamento_id, duracao_s),
    }


def _eventos_refrigeracao(rng, dia, loja_id, loja, equipamento_id, alertas_ativos):
    perfil = perfil_do_equipamento(equipamento_id)
    horas = list(range(loja["abre"], loja["fecha"]))
    pesos = [_peso_hora(h) for h in horas]
    eventos = []
    for _ in range(rng.randint(*perfil["eventos_dia"])):
        hora = rng.choices(horas, weights=pesos, k=1)[0]
        inicio = datetime(dia.year, dia.month, dia.day, hora, rng.randint(0, 59), rng.randint(0, 59))
        duracao = int(rng.lognormvariate(math.log(perfil["mediana_s"]), 0.6))
        duracao = max(15, min(duracao, 1800))
        if alertas_ativos and duracao > LIMITE_ALERTA_S and rng.random() < PROB_ALERTA_RESOLVIDO:
            duracao = LIMITE_ALERTA_S + rng.randint(15, 60)
        eventos.append(_evento(rng, loja_id, equipamento_id, inicio, duracao))
    return eventos


def _eventos_iluminacao(rng, dia, loja_id, loja, equipamento_id, alertas_ativos):
    eventos = []
    if equipamento_id == "iluminacao-estoque":
        # Estoque vazio com a luz acesa, algumas vezes ao longo do expediente.
        for _ in range(rng.randint(1, 3)):
            hora = rng.randint(loja["abre"], loja["fecha"] - 1)
            inicio = datetime(dia.year, dia.month, dia.day, hora, rng.randint(0, 59), 0)
            duracao = rng.randint(600, 3600)
            if alertas_ativos and rng.random() < PROB_ALERTA_RESOLVIDO:
                duracao = rng.randint(120, 420)
            eventos.append(_evento(rng, loja_id, equipamento_id, inicio, duracao))
    else:
        # Salão: luz esquecida acesa depois de fechar, em cerca de metade dos dias.
        if rng.random() < 0.5:
            inicio = datetime(dia.year, dia.month, dia.day, loja["fecha"], rng.randint(0, 20), 0)
            duracao = rng.randint(1200, 7200)
            if alertas_ativos and rng.random() < PROB_ALERTA_RESOLVIDO:
                duracao = rng.randint(120, 600)
            eventos.append(_evento(rng, loja_id, equipamento_id, inicio, duracao))
    return eventos


def _registros_defeituosos(rng, dia, eventos_bons):
    """Hipótese 4: defeitos propositais, um de cada tipo em rodízio."""
    quantidade = max(1, round(len(eventos_bons) * TAXA_REGISTROS_DEFEITUOSOS))
    defeituosos = []
    for i in range(quantidade):
        base = dict(rng.choice(eventos_bons))
        defeito = (dia.toordinal() + i) % 5  # o rodízio continua de um dia para o outro
        if defeito == 0:  # duração inválida
            base["id"] = _uuid(rng)
            base["duracao_s"] = rng.choice([0, -5])
            base["custo_estimado_rs"] = 0.0
        elif defeito == 1:  # loja que não existe no cadastro
            base["id"] = _uuid(rng)
            base["loja_id"] = "loja-99"
        elif defeito == 2:  # início no futuro (relógio do dispositivo errado)
            base["id"] = _uuid(rng)
            futuro = datetime(dia.year, dia.month, dia.day, 12, 0, 0) + timedelta(days=400)
            base["inicio"] = _iso(futuro)
        elif defeito == 3:  # equipamento ausente
            base["id"] = _uuid(rng)
            base["equipamento_id"] = None
        else:  # mesmo evento enviado duas vezes (mantém o id)
            pass
        defeituosos.append(base)
    return defeituosos


def gerar_eventos(ds: str) -> list[dict]:
    """Gera os eventos de um dia (ds = 'AAAA-MM-DD'). Determinístico por dia."""
    dia = date.fromisoformat(ds)
    rng = random.Random(f"energia-que-da-lucro:{VERSAO}:{ds}")
    alertas_ativos = dia >= DATA_INICIO_ALERTAS

    eventos = []
    for loja_id, loja in LOJAS.items():
        for equipamento_id in loja["equipamentos"]:
            if equipamento_id.startswith("iluminacao"):
                gerador = _eventos_iluminacao
            else:
                gerador = _eventos_refrigeracao
            eventos.extend(gerador(rng, dia, loja_id, loja, equipamento_id, alertas_ativos))

    eventos.sort(key=lambda e: e["inicio"])
    eventos.extend(_registros_defeituosos(rng, dia, eventos))
    return eventos


def escrever_jsonl(eventos: list[dict], destino: Path) -> int:
    destino.parent.mkdir(parents=True, exist_ok=True)
    with destino.open("w", encoding="utf-8") as arquivo:
        for evento in eventos:
            arquivo.write(json.dumps(evento, ensure_ascii=False) + "\n")
    return len(eventos)


if __name__ == "__main__":
    # Uso fora do Airflow: python simulador_eventos.py 2026-11-08 [pasta_destino]
    import sys

    dia_arg = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()
    pasta = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("chegada") / "eventos"
    total = escrever_jsonl(gerar_eventos(dia_arg), pasta / f"eventos_{dia_arg}.json")
    print(f"{total} eventos gravados em {pasta / f'eventos_{dia_arg}.json'}")

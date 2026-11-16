"""Carrega a fato da Ouro no MVP (painel do comerciante) pela API dele.

Lê amostras/ouro/fato_desperdicio_ouro.csv, exportado de
workspace.energia_lucro.fato_desperdicio com as dimensões de loja e equipamento,
e envia cada linha para POST /events do painel (github.com/Natan-TI/devops-energia).

Uso:
    API_KEY=<chave do painel> python scripts/carrega_ouro_no_mvp.py [http://localhost:8000]

Adaptações ao contrato de entrada do painel:
- tipo "luz_acesa_sem_necessidade" vira "luz_acesa";
- inicio usa a hora de referência da fato (a Ouro guarda o evento por hora), em Brasília;
- custo_estimado_rs recebe custo_aneel_rs: o painel calcula kWh = custo / 0,789
  (tarifa B3 da ANEEL), então os kWh exibidos ficam iguais aos da Ouro.
A chave vem da variável de ambiente; nada sensível fica no código.
"""
import csv
import json
import os
import sys
import urllib.request
from pathlib import Path

ARQUIVO = Path(__file__).resolve().parent.parent / "amostras" / "ouro" / "fato_desperdicio_ouro.csv"
TIPOS = {"porta_aberta": "porta_aberta", "luz_acesa_sem_necessidade": "luz_acesa"}


def main() -> None:
    base = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
    chave = os.environ["API_KEY"]
    novos = repetidos = 0
    with ARQUIVO.open(encoding="utf-8-sig", newline="") as f:
        for linha in csv.DictReader(f):
            evento = {
                "id": linha["evento_id"],
                "loja_id": linha["loja_id"],
                "equipamento_id": linha["equipamento_id"],
                "tipo": TIPOS[linha["tipo"]],
                "origem": linha["origem"],
                "inicio": f"{linha['data']}T{int(linha['hora']):02d}:00:00-03:00",
                "duracao_s": int(linha["duracao_s"]),
                "custo_estimado_rs": float(linha["custo_aneel_rs"]),
            }
            pedido = urllib.request.Request(
                f"{base}/events",
                data=json.dumps(evento).encode(),
                headers={"Content-Type": "application/json", "X-API-Key": chave},
            )
            with urllib.request.urlopen(pedido, timeout=10) as resposta:
                if resposta.status == 201:
                    novos += 1
                else:
                    repetidos += 1
    print(f"Eventos da Ouro enviados ao painel: {novos} novos, {repetidos} já existentes.")


if __name__ == "__main__":
    main()

#!/usr/bin/env bash
# Sobe o Airflow local (scheduler, API e interface) em http://localhost:8080
# Rode antes, uma vez:  bash scripts/setup_airflow.sh
set -euo pipefail

ENV_FILE="$HOME/airflow-gs/env.sh"
if [ ! -f "$ENV_FILE" ]; then
  echo "Ambiente não encontrado. Rode primeiro: bash scripts/setup_airflow.sh" >&2
  exit 1
fi
# shellcheck disable=SC1090
source "$ENV_FILE"

echo "Interface: http://localhost:8080  (Ctrl+C para parar)"
exec airflow standalone

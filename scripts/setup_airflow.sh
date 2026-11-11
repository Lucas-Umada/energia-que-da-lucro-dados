#!/usr/bin/env bash
# Prepara um Airflow 3 local (Linux ou WSL) para rodar o DAG gs_energia_coleta.
# Uso:  bash scripts/setup_airflow.sh [pasta_de_chegada]
# Não precisa de sudo: o Python vem pelo uv, dentro da sua pasta de usuário.
set -euo pipefail

AIRFLOW_VERSION="3.4.0"
PYTHON_VERSION="3.12"

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHEGADA="${1:-$REPO_DIR/chegada}"
export AIRFLOW_HOME="$HOME/airflow-gs"
VENV="$AIRFLOW_HOME/venv"

mkdir -p "$AIRFLOW_HOME" "$CHEGADA"
export PATH="$HOME/.local/bin:$PATH"

if ! command -v uv >/dev/null 2>&1; then
  echo ">> Instalando o uv (gerenciador de Python)..."
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi

echo ">> Criando o ambiente Python $PYTHON_VERSION em $VENV"
uv venv --python "$PYTHON_VERSION" "$VENV"

echo ">> Instalando o Airflow $AIRFLOW_VERSION (pode levar alguns minutos)"
uv pip install --python "$VENV/bin/python" \
  "apache-airflow==$AIRFLOW_VERSION" requests \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-$PYTHON_VERSION.txt"

# Configuração local, carregada pelo start_airflow.sh. Sem segredos.
cat > "$AIRFLOW_HOME/env.sh" <<EOF
export AIRFLOW_HOME="$AIRFLOW_HOME"
export AIRFLOW__CORE__DAGS_FOLDER="$REPO_DIR/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DEFAULT_TIMEZONE=America/Sao_Paulo
# Uso local, só em localhost: abre a interface sem tela de login.
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True
export PATH="$VENV/bin:\$PATH"
EOF

# shellcheck disable=SC1091
source "$AIRFLOW_HOME/env.sh"

echo ">> Criando o banco do Airflow e as Variables"
airflow db migrate
airflow variables set gs_chegada_dir "$CHEGADA"
airflow variables set gs_latitude "-23.55"
airflow variables set gs_longitude "-46.63"
airflow variables set gs_distribuidora "ELETROPAULO"

echo
echo "Pronto. Pasta de chegada: $CHEGADA"
echo "Para subir o Airflow:  bash scripts/start_airflow.sh"

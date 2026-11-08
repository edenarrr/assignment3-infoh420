#!/usr/bin/env bash
# Source this file from the project root, after activating the virtual environment.
if [[ ! -f .env ]]; then
    echo 'Copy .env.example to .env and fill in your configuration first.' >&2
    return 1
fi
source .env
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__PARALLELISM=4
export PYTHONPATH="$PWD/dags${PYTHONPATH:+:$PYTHONPATH}"

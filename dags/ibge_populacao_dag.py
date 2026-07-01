"""DAG de ingestão anual da população estimada por município (IBGE).

Roda anualmente (o IBGE publica a estimativa populacional uma vez por
ano, geralmente em agosto — agendar mais frequente rodaria a DAG sem
nunca encontrar dado novo). O ano de referência é um `param` da DAG
(default: ano corrente), então dá pra disparar manualmente pra
backfill de anos anteriores sem editar código.

Fluxo: extract (bate na API do IBGE, salva JSON cru local) -> load (lê
o JSON salvo, sem bater na rede de novo, e carrega no Postgres) -> dbt
run/test.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

# garante que `src/` é importável de dentro do container/worker do Airflow
sys.path.append(str(Path(__file__).resolve().parents[1]))

DBT_PROJECT_DIR = str(Path(__file__).resolve().parents[1] / "dbt" / "ibge_analytics")

default_args = {
    "owner": "joao.batista",
    "retries": 2,
    "retry_delay": 300,  # segundos
}


@dag(
    dag_id="ibge_populacao_pipeline",
    description="Extrai, carrega e transforma a população estimada por município (IBGE)",
    schedule="@yearly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    params={"ano": Param(default=datetime.now().year, type="integer", description="Ano de referência a extrair")},
    tags=["ibge", "extraction", "portfolio"],
)
def ibge_populacao_pipeline():

    @task
    def extract_municipios() -> str:
        from src.extract.ibge_populacao import fetch_municipios_raw, save_raw_json

        raw = fetch_municipios_raw()
        return str(save_raw_json(raw, nome="municipios"))

    @task
    def extract_populacao(**context) -> str:
        from src.extract.ibge_populacao import fetch_populacao_raw, save_raw_json

        ano = context["params"]["ano"]
        raw = fetch_populacao_raw(ano)
        return str(save_raw_json(raw, nome="populacao", particao=f"ano={ano}"))

    @task
    def load(municipios_path: str, populacao_path: str, **context) -> None:
        import json

        from src.extract.ibge_populacao import parse_municipios, parse_populacao
        from src.load.load_to_postgres import ensure_schema, get_engine, load_municipios, load_populacao

        ano = context["params"]["ano"]
        municipios_raw = json.loads(Path(municipios_path).read_text(encoding="utf-8"))
        populacao_raw = json.loads(Path(populacao_path).read_text(encoding="utf-8"))

        engine = get_engine()
        ensure_schema(engine)
        load_municipios(engine, df=parse_municipios(municipios_raw))
        load_populacao(engine, ano, df=parse_populacao(populacao_raw, ano))

    # ATENÇÃO: estes dois tasks assumem `dbt` disponível no PATH do worker.
    # No docker-compose local deste projeto ele NÃO está (ver comentário
    # no serviço `airflow`) — instalar dbt-core no mesmo ambiente do
    # Airflow quebra o ORM interno dele (sqlalchemy 2.0 vs o <2.0 que o
    # Airflow 2.9 exige). Em produção isso viraria um KubernetesPodOperator
    # ou uma imagem custom com dbt pré-instalado, separada da imagem do
    # Airflow. Por enquanto rodo `dbt run`/`dbt test` via CLI direto
    # (ver README) e deixo só extract/load orquestrados pelo Airflow.
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
    )

    municipios_path = extract_municipios()
    populacao_path = extract_populacao()
    loaded = load(municipios_path, populacao_path)
    loaded >> dbt_run >> dbt_test


ibge_populacao_pipeline()

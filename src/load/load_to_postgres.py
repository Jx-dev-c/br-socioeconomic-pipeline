"""Carrega os dados extraídos do IBGE para a zona `raw` do Postgres.

Este é o estágio de Load explícito entre extração e dbt: o dbt só lê de
tabelas já carregadas (`raw.ibge_municipios`, `raw.ibge_populacao`), nunca
de arquivo. Duas estratégias diferentes por tabela, cada uma justificada
pelo formato do dado:

- `ibge_municipios` é uma dimensão pequena (~5.6k linhas) que representa o
  estado atual da divisão territorial: full-refresh a cada carga, sem
  necessidade de histórico. Importante: é TRUNCATE + insert, não
  drop-and-recreate (`if_exists="replace"` do pandas) — assim que o dbt
  cria a view `stg_ibge_municipios` em cima dessa tabela, um DROP TABLE
  passa a falhar com `DependentObjectsStillExist` (visto de verdade
  rodando a DAG no Airflow depois de já ter rodado `dbt run` uma vez).
  TRUNCATE preserva o objeto da tabela e as views que dependem dela.
- `ibge_populacao` cresce por ano: usamos delete-then-insert do ano sendo
  carregado, o que torna a carga idempotente (reprocessar o mesmo ano não
  duplica linhas) sem precisar de um MERGE de verdade.
"""
from __future__ import annotations

import argparse
import logging

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from src.config import config
from src.extract.ibge_populacao import fetch_municipios, fetch_populacao

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RAW_SCHEMA = "raw"


def get_engine() -> Engine:
    if not config.database_url:
        raise ValueError("DATABASE_URL não configurada. Preencha o .env antes de carregar dados.")
    return create_engine(config.database_url)


def ensure_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}"))


def load_municipios(engine: Engine, df: pd.DataFrame | None = None) -> int:
    df = df if df is not None else fetch_municipios()

    if inspect(engine).has_table("ibge_municipios", schema=RAW_SCHEMA):
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {RAW_SCHEMA}.ibge_municipios"))

    df.to_sql("ibge_municipios", engine, schema=RAW_SCHEMA, if_exists="append", index=False)
    logger.info("raw.ibge_municipios recarregada: %d linhas", len(df))
    return len(df)


def load_populacao(engine: Engine, ano: int, df: pd.DataFrame | None = None) -> int:
    df = df if df is not None else fetch_populacao(ano)

    if inspect(engine).has_table("ibge_populacao", schema=RAW_SCHEMA):
        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM {RAW_SCHEMA}.ibge_populacao WHERE ano = :ano"), {"ano": ano})

    df.to_sql("ibge_populacao", engine, schema=RAW_SCHEMA, if_exists="append", index=False)
    logger.info("raw.ibge_populacao: %d linhas carregadas para o ano %d", len(df), ano)
    return len(df)


def run(anos: list[int]) -> None:
    engine = get_engine()
    ensure_schema(engine)
    load_municipios(engine)
    for ano in anos:
        load_populacao(engine, ano)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Carrega dados do IBGE para o Postgres (zona raw)")
    parser.add_argument("--anos", type=str, default=None, help="Anos separados por vírgula, ex: 2023,2024")
    args = parser.parse_args()

    anos_arg = [int(a) for a in args.anos.split(",")] if args.anos else config.ibge_anos
    run(anos=anos_arg)

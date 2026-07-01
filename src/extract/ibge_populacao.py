"""Extração de dados socioeconômicos do IBGE (API pública, sem autenticação).

Duas fontes:
- Localidades (`/api/v1/localidades/municipios`): lista de municípios com
  hierarquia geográfica completa (UF, região) — usada como dimensão.
- Agregados/SIDRA (`/api/v3/agregados/6579/...`): população residente
  estimada por município e ano (tabela 6579, variável 9324) — usada como
  fato.

Ambos os endpoints foram testados manualmente antes de escrever este
módulo (ao contrário do dataset da PRF, não há URL para "adivinhar":
o formato é estável e documentado).
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MUNICIPIOS_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
POPULACAO_URL_TEMPLATE = (
    "https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/{ano}/variaveis/9324"
)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8), reraise=True)
def _get_json(url: str, params: dict | None = None) -> Any:
    """GET com retry exponencial. A API do IBGE é estável, mas pública e sem
    SLA — três tentativas cobrem falhas transitórias sem mascarar erro real."""
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_municipios_raw() -> list[dict]:
    """Busca o JSON cru da lista de municípios brasileiros."""
    return _get_json(MUNICIPIOS_URL)


def parse_municipios(raw: list[dict]) -> pd.DataFrame:
    """Achata a hierarquia geográfica (UF, região) de cada município.

    Normalmente a UF vem em `microrregiao.mesorregiao.UF`, mas pelo menos
    um município (Boa Esperança do Norte/MT, id 5101837) tem
    `microrregiao: null` na API — nesse caso cai para o caminho
    equivalente via `regiao-imediata.regiao-intermediaria.UF`."""
    rows = []
    for municipio in raw:
        if municipio["microrregiao"] is not None:
            uf = municipio["microrregiao"]["mesorregiao"]["UF"]
        else:
            uf = municipio["regiao-imediata"]["regiao-intermediaria"]["UF"]
        regiao = uf["regiao"]
        rows.append(
            {
                "municipio_id": municipio["id"],
                "municipio_nome": municipio["nome"],
                "uf_id": uf["id"],
                "uf_sigla": uf["sigla"],
                "uf_nome": uf["nome"],
                "regiao_id": regiao["id"],
                "regiao_sigla": regiao["sigla"],
                "regiao_nome": regiao["nome"],
            }
        )
    return pd.DataFrame(rows)


def fetch_municipios() -> pd.DataFrame:
    return parse_municipios(fetch_municipios_raw())


def fetch_populacao_raw(ano: int) -> Any:
    """Busca o JSON cru da população estimada de todos os municípios num ano."""
    url = POPULACAO_URL_TEMPLATE.format(ano=ano)
    return _get_json(url, params={"localidades": "N6[all]"})


def parse_populacao(raw: Any, ano: int) -> pd.DataFrame:
    """Converte a resposta do SIDRA (uma série por município) em formato
    longo: uma linha por município/ano. Municípios sem valor publicado pro
    ano (marcados como '-', '...' ou 'X' pelo IBGE) são descartados.

    Anos de Censo (ex: 2022) não têm estimativa nessa tabela — a API
    retorna `[]` em vez de uma série. Nesse caso devolve DataFrame vazio
    com o schema certo, em vez de estourar IndexError."""
    if not raw:
        return pd.DataFrame(columns=["municipio_id", "ano", "populacao_estimada"])

    series = raw[0]["resultados"][0]["series"]
    rows = [
        {
            "municipio_id": int(item["localidade"]["id"]),
            "ano": ano,
            "populacao_estimada": int(item["serie"][str(ano)]),
        }
        for item in series
        if item["serie"].get(str(ano)) not in (None, "-", "...", "X")
    ]
    return pd.DataFrame(rows)


def fetch_populacao(ano: int) -> pd.DataFrame:
    return parse_populacao(fetch_populacao_raw(ano), ano)


def save_raw_json(payload: Any, nome: str, particao: str | None = None, dest_dir: Path | None = None) -> Path:
    """Salva o JSON cru (não transformado) em `./data/raw/ibge/{nome}/...`.

    `particao` é usado para dados que crescem no tempo (ex: `ano=2024`
    para população). Dimensões como municípios não recebem partição —
    cada extração sobrescreve o snapshot mais atual."""
    base_dir = (dest_dir or config.local_data_dir) / "ibge" / nome
    if particao:
        base_dir = base_dir / particao
    base_dir.mkdir(parents=True, exist_ok=True)

    path = base_dir / f"{nome}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    logger.info("JSON salvo em %s", path)
    return path


def upload_to_s3(local_path: Path, key: str) -> str:
    """Sobe o arquivo raw para o S3."""
    import boto3

    s3 = boto3.client(
        "s3",
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
        region_name=config.aws_region,
    )
    s3.upload_file(str(local_path), config.s3_bucket, key)
    uri = f"s3://{config.s3_bucket}/{key}"
    logger.info("Upload concluído: %s", uri)
    return uri


def run(anos: list[int], upload: bool = False) -> None:
    municipios_raw = fetch_municipios_raw()
    municipios_path = save_raw_json(municipios_raw, nome="municipios")
    logger.info("Municípios extraídos: %d", len(municipios_raw))
    if upload:
        upload_to_s3(municipios_path, key="raw/ibge/municipios/municipios.json")

    for ano in anos:
        populacao_raw = fetch_populacao_raw(ano)
        populacao_path = save_raw_json(populacao_raw, nome="populacao", particao=f"ano={ano}")
        logger.info("População extraída para %d", ano)
        if upload:
            upload_to_s3(populacao_path, key=f"raw/ibge/populacao/ano={ano}/populacao.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extrai dados socioeconômicos do IBGE")
    parser.add_argument("--anos", type=str, default=None, help="Anos separados por vírgula, ex: 2023,2024")
    parser.add_argument("--upload", action="store_true", help="Fazer upload para S3 após extrair")
    args = parser.parse_args()

    anos_arg = [int(a) for a in args.anos.split(",")] if args.anos else config.ibge_anos
    run(anos=anos_arg, upload=args.upload)

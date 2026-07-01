"""Configuração central do projeto, lida a partir de variáveis de ambiente."""
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv
import os

load_dotenv()


def _parse_anos(raw: str) -> list[int]:
    return [int(ano.strip()) for ano in raw.split(",") if ano.strip()]


@dataclass
class Config:
    aws_access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID", "")
    aws_secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")
    s3_bucket: str = os.getenv("S3_BUCKET", "")

    ibge_anos: list[int] = field(default_factory=lambda: _parse_anos(os.getenv("IBGE_ANOS", "2024")))

    database_url: str = os.getenv("DATABASE_URL", "")

    # diretório local usado antes do upload para S3 (útil pra testar sem AWS)
    local_data_dir: Path = Path(os.getenv("LOCAL_DATA_DIR", "./data/raw"))


config = Config()

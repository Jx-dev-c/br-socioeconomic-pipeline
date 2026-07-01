"""Testes do módulo de extração do IBGE.

Importante: nunca bater na rede de verdade em teste automatizado. Os
testes de rede usam `mocker` (pytest-mock) para simular a resposta do
`requests.get`. Os fixtures abaixo reproduzem o formato real observado
manualmente na API (incluindo o caso de município sem `microrregiao` e
valores de população não publicados, marcados como '...' pelo IBGE).
"""
import requests

from src.extract import ibge_populacao


def _municipio(id_, nome, com_microrregiao=True):
    uf = {
        "id": 11,
        "sigla": "RO",
        "nome": "Rondônia",
        "regiao": {"id": 1, "sigla": "N", "nome": "Norte"},
    }
    if com_microrregiao:
        return {
            "id": id_,
            "nome": nome,
            "microrregiao": {"id": 1, "nome": "Microrregião X", "mesorregiao": {"id": 1, "nome": "Mesorregião X", "UF": uf}},
        }
    return {
        "id": id_,
        "nome": nome,
        "microrregiao": None,
        "regiao-imediata": {"id": 1, "nome": "Regiao Y", "regiao-intermediaria": {"id": 1, "nome": "Regiao Z", "UF": uf}},
    }


def test_parse_municipios_com_microrregiao():
    raw = [_municipio(1100015, "Alta Floresta D'Oeste")]

    df = ibge_populacao.parse_municipios(raw)

    assert list(df.columns) == [
        "municipio_id",
        "municipio_nome",
        "uf_id",
        "uf_sigla",
        "uf_nome",
        "regiao_id",
        "regiao_sigla",
        "regiao_nome",
    ]
    row = df.iloc[0]
    assert row["municipio_id"] == 1100015
    assert row["uf_sigla"] == "RO"
    assert row["regiao_sigla"] == "N"


def test_parse_municipios_sem_microrregiao_usa_fallback():
    """Pelo menos um município real (Boa Esperança do Norte/MT) vem com
    `microrregiao: null` na API — precisa cair pro caminho alternativo."""
    raw = [_municipio(5101837, "Boa Esperança do Norte", com_microrregiao=False)]

    df = ibge_populacao.parse_municipios(raw)

    assert df.iloc[0]["uf_sigla"] == "RO"


def test_parse_populacao_formato_longo():
    raw = [
        {
            "id": "9324",
            "variavel": "População residente estimada",
            "resultados": [
                {
                    "series": [
                        {"localidade": {"id": "3550308"}, "serie": {"2024": "12396372"}},
                        {"localidade": {"id": "1100015"}, "serie": {"2024": "22853"}},
                    ]
                }
            ],
        }
    ]

    df = ibge_populacao.parse_populacao(raw, ano=2024)

    assert len(df) == 2
    assert set(df.columns) == {"municipio_id", "ano", "populacao_estimada"}
    assert df[df["municipio_id"] == 3550308]["populacao_estimada"].iloc[0] == 12396372


def test_parse_populacao_ano_sem_dado_retorna_dataframe_vazio():
    """Anos de Censo (ex: 2022 de verdade, confirmado contra a API) não
    têm estimativa nessa tabela — a API retorna `[]` em vez de uma série.
    Isso não pode virar IndexError, tem que virar um DataFrame vazio com
    o schema certo."""
    df = ibge_populacao.parse_populacao([], ano=2022)

    assert list(df.columns) == ["municipio_id", "ano", "populacao_estimada"]
    assert len(df) == 0


def test_parse_populacao_descarta_valores_nao_publicados():
    """O IBGE marca ano sem estimativa publicada pro município com '...'
    (visto de verdade pra Boa Esperança do Norte/MT em 2024) — essas
    linhas devem ser descartadas, não viram população 0 ou NaN na fato."""
    raw = [
        {
            "resultados": [
                {
                    "series": [
                        {"localidade": {"id": "5101837"}, "serie": {"2024": "..."}},
                        {"localidade": {"id": "1100015"}, "serie": {"2024": "22853"}},
                    ]
                }
            ]
        }
    ]

    df = ibge_populacao.parse_populacao(raw, ano=2024)

    assert len(df) == 1
    assert df.iloc[0]["municipio_id"] == 1100015


def test_get_json_faz_retry_e_propaga_erro_apos_esgotar(monkeypatch, mocker):
    monkeypatch.setattr("time.sleep", lambda *a, **k: None)  # não esperar de verdade no teste

    mocker.patch(
        "src.extract.ibge_populacao.requests.get",
        side_effect=requests.exceptions.ConnectionError("timeout simulado"),
    )

    try:
        ibge_populacao._get_json("https://exemplo.gov.br/fake")
        assert False, "deveria ter levantado ConnectionError"
    except requests.exceptions.ConnectionError:
        pass

    assert ibge_populacao.requests.get.call_count == 3  # stop_after_attempt(3)


def test_save_raw_json_particiona_por_ano(tmp_path):
    path = ibge_populacao.save_raw_json({"a": 1}, nome="populacao", particao="ano=2024", dest_dir=tmp_path)

    assert path == tmp_path / "ibge" / "populacao" / "ano=2024" / "populacao.json"
    assert path.exists()
    assert "1" in path.read_text(encoding="utf-8")


def test_save_raw_json_sem_particao_para_dimensao(tmp_path):
    path = ibge_populacao.save_raw_json([{"id": 1}], nome="municipios", dest_dir=tmp_path)

    assert path == tmp_path / "ibge" / "municipios" / "municipios.json"

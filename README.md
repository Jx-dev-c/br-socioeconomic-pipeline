# BR Socioeconomic Pipeline

Pipeline de dados end-to-end sobre população estimada dos municípios
brasileiros, usando a API pública do IBGE. Projeto de portfólio
construído para demonstrar habilidades de engenharia de dados:
extração de API real, orquestração, modelagem dimensional e qualidade
de dados.

## Fonte de dados

API pública do IBGE (`servicodados.ibge.gov.br`), sem autenticação:

- [`/api/v1/localidades/municipios`](https://servicodados.ibge.gov.br/api/v1/localidades/municipios) —
  5.571 municípios com hierarquia geográfica completa (UF, região).
- [`/api/v3/agregados/6579`](https://sidra.ibge.gov.br/tabela/6579) —
  população residente estimada por município, anual, 2001–2025
  (tabela SIDRA 6579, variável 9324).

Licença: dado público federal, uso livre. Ambos os endpoints foram
testados manualmente antes de codar a extração — diferente de um
portal que expõe só arquivos pra download, essa é uma API REST estável
e documentada, sem URL pra "adivinhar".

## Arquitetura

```
Fonte (API IBGE)
  -> Extração (Python, salva JSON cru localmente/S3, particionado por ano)
  -> Load (Postgres, schema raw)
  -> Orquestração (Airflow, agendamento anual)
  -> Transformação (dbt: staging -> marts, star schema)
  -> Consumo (Postgres/warehouse + dashboard BI)
```

## Stack

- **Extração**: Python (requests, pandas, tenacity para retry)
- **Armazenamento raw**: local (`./data/raw/`) ou AWS S3, opcional
- **Load**: SQLAlchemy, direto para Postgres
- **Orquestração**: Apache Airflow
- **Transformação**: dbt
- **Warehouse**: PostgreSQL
- **Testes**: pytest (mocka toda chamada de rede) + testes nativos do dbt

## Modelo de dados (star schema)

- `dim_municipio` — 1 linha por município, com UF e região
- `dim_tempo` — 1 linha por ano presente nos dados de população
- `fct_populacao` — 1 linha por (município, ano), métrica: `populacao_estimada`

## Estrutura do projeto

```
br-socioeconomic-pipeline/
├── src/extract/            # extração da API do IBGE
├── src/load/                # carga da zona raw para o Postgres
├── dags/                    # DAG do Airflow
├── dbt/ibge_analytics/      # projeto dbt (staging + marts)
├── tests/                   # testes automatizados (mockados)
└── docs/                    # documentação de arquitetura
```

## Como rodar localmente

```bash
# 1. entrar no projeto
cd br-socioeconomic-pipeline

# 2. subir a infra: Postgres, Airflow e Metabase
docker compose up -d
# Postgres: localhost:5433 | Airflow: localhost:8080 | Metabase: localhost:3000

# 3. ambiente virtual + dependências (use Python 3.11/3.12 — dbt ainda
# não suporta 3.13+)
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 4. configurar variáveis de ambiente
cp .env.example .env
# IBGE não precisa de chave — DATABASE_URL já aponta pro Postgres do compose

# 5. rodar a extração (salva em ./data/raw/, não precisa do Postgres rodando)
python -m src.extract.ibge_populacao --anos 2024,2025

# 6. carregar no Postgres
python -m src.load.load_to_postgres --anos 2024,2025

# 7. rodar dbt
cd dbt/ibge_analytics
dbt deps         # instala dbt_utils
dbt debug        # confirma conexão com o banco
dbt run
dbt test
```

A partir daí: dispare a DAG `ibge_populacao_pipeline` em
`localhost:8080` (usuário `admin`, senha gerada na primeira subida —
ver `docker logs ibge_airflow` ou o arquivo
`standalone_admin_password.txt` dentro do container), e monte os
gráficos em `localhost:3000` conectando no banco `ibge_dw`.

## Decisões e trade-offs

- **Por que IBGE em vez de um portal de download (como a PRF)?**
  A primeira versão deste projeto usava os dados de acidentes da PRF,
  mas a URL do arquivo ZIP no portal muda de formato sem aviso e não
  dá pra confirmar via automação — é um risco real de quebrar numa
  demonstração ao vivo, fora do meu controle. A API do IBGE é REST,
  documentada, estável e sem autenticação: prioriza confiabilidade de
  demo, com um custo pequeno de "história" (dado de governo mais
  arrumado do que um CSV de acidentes).

- **Por que um estágio de Load explícito, separado da extração?**
  A extração só sabe falar com a API do IBGE e salvar JSON cru; o
  Load só sabe ler esse JSON e escrever no Postgres. Isso significa
  que reprocessar uma carga (ex: um schema novo no banco) não exige
  bater na API de novo — só reler o arquivo já salvo. A DAG do
  Airflow reflete essa separação: `extract` roda antes e não depende
  do banco, `load` roda depois e não depende de rede.

- **Por que delete-then-insert em `ibge_populacao` em vez de um MERGE de
  verdade?** O volume é pequeno (~5.6 mil linhas por ano) e a carga é
  sempre por ano inteiro, nunca incremental dentro de um ano — apagar
  e reinserir o ano é simples, idempotente (rodar duas vezes não
  duplica) e fácil de auditar. Um MERGE ganharia sentido se a carga
  fosse por município individual ou muito mais frequente.

- **Por que `dim_municipio` é full-refresh via TRUNCATE + insert, e não
  `if_exists="replace"` do pandas (que faz DROP TABLE + CREATE)?** A
  tabela representa a divisão territorial vigente, sem necessidade de
  histórico (SCD) — mas descobri rodando a DAG de verdade no Airflow,
  depois de já ter rodado `dbt run` uma vez, que `DROP TABLE` falha com
  `DependentObjectsStillExist` assim que a view `stg_ibge_municipios`
  passa a depender da tabela raw. TRUNCATE preserva o objeto da tabela
  (e as views que dependem dela) e resolve o mesmo problema sem esse
  acoplamento frágil entre load e dbt.

- **Por que agendamento anual (`@yearly`) no Airflow, e não mensal ou
  diário?** O IBGE publica a estimativa populacional uma vez por ano
  (geralmente em agosto). Agendar com mais frequência rodaria a DAG
  sem nunca encontrar dado novo — desperdício de execução e ruído nos
  logs, mesmo raciocínio que já vale pra qualquer fonte com
  publicação pouco frequente.

- **Por que grão município + ano no fato, em vez de manter os dados
  "largos" (uma coluna por ano)?** Formato longo é o que star schema
  espera: cada nova carga de ano é só uma inserção, sem precisar
  alterar o schema da tabela (`ALTER TABLE` pra adicionar coluna).

## Qualidade de dados: casos reais encontrados

- Um município (Boa Esperança do Norte/MT, id `5101837`) apareceu em
  `dim_municipio` mas sem linha em `fct_populacao` para 2024 — o IBGE
  simplesmente não publicou estimativa pra ele nesse ano (a API
  retorna `"..."` no lugar do valor). A extração já descarta esses
  casos (`parse_populacao` em `src/extract/ibge_populacao.py`), e por
  isso o teste dbt de `relationships` roda só a partir do fato pras
  dimensões, nunca o contrário — não é razoável exigir que toda
  dimensão tenha fato correspondente em todo ano.

- **2022 e 2023 não têm estimativa nenhuma** nessa tabela (2022 foi
  ano de Censo — o IBGE publica a contagem oficial em outra tabela, e
  a série de estimativas anuais retoma em 2024). A API retorna uma
  lista vazia `[]` pro período inteiro, não um valor por município.
  Descobri isso rodando a carga de verdade contra o Postgres local: o
  código quebrava com `IndexError` ao tentar indexar uma resposta
  vazia. Corrigido pra devolver um DataFrame vazio nesse caso — e por
  isso `IBGE_ANOS` no `.env.example` usa `2024,2025`, não `2023,2024`.

## Roadmap

Já rodou de ponta a ponta: Postgres, extração + load, dbt (5 models,
15 testes), Airflow (extract/load orquestrados de verdade) e Metabase
com os primeiros gráficos. Próximos passos:

- [ ] Adicionar um segundo indicador do IBGE via a mesma API de
      agregados (ex: PIB municipal) — o padrão de
      `fetch_*_raw`/`parse_*` já é reutilizável
- [ ] CI simples (GitHub Actions) rodando `pytest` a cada push
- [ ] Imagem Docker custom com dbt pré-instalado, pra rodar
      `dbt_run`/`dbt_test` também dentro do Airflow (hoje só funcionam
      via CLI direto — ver comentário no `docker-compose.yml`)

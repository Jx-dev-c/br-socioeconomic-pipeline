# Arquitetura

## Visão geral

```
Fonte de dados         Extração              Load                Orquestração        Transformação        Consumo
(API IBGE)          -> (Python, JSON raw  -> (Postgres,        -> (Airflow,        -> (dbt: staging/    -> (Warehouse + BI)
                        local/S3, part.       schema raw)          anual)              marts)
                        por ano)
```

## Por que cada escolha

- **JSON cru como raw antes do banco**: permite reprocessar do zero se
  um modelo dbt mudar ou o schema do Postgres precisar recriar, sem
  bater na API do IBGE de novo. Deixa claro, na auditoria, o que veio
  exatamente da fonte vs o que foi transformado.
- **Particionamento por ano no raw**: a própria tabela do IBGE
  (SIDRA 6579) é publicada por ano — seguir essa granularidade evita
  reprocessar anos que não mudaram.
- **Load como estágio explícito, separado da extração**: a extração só
  fala com a API e escreve arquivo; o load só lê arquivo e escreve no
  banco. Nenhum dos dois lados precisa saber do outro além do caminho
  do arquivo — facilita testar cada um isoladamente.
- **Airflow com agendamento anual, não mensal ou diário**: a fonte
  (estimativa populacional) só é atualizada uma vez por ano. Agendar
  mais frequente rodaria a DAG sem nunca encontrar dado novo —
  desperdício de execução e ruído nos logs.
- **dbt para staging + marts**: separa "limpar dado" de "modelar
  negócio". Staging é descartável e barato de recriar; marts carregam
  a lógica que interessa pro consumo final.

## Executando localmente (visão rápida)

1. Postgres local via Docker, pra servir de warehouse
2. `python -m src.extract.ibge_populacao --anos 2024,2025` — extração
   manual fora do Airflow, útil durante o desenvolvimento (não precisa
   de Postgres nem AWS pra rodar, só salva JSON local)
3. `python -m src.load.load_to_postgres --anos 2024,2025` — carrega o
   que foi extraído (precisa de `DATABASE_URL` configurada)
4. `dbt run` + `dbt test` dentro de `dbt/ibge_analytics`
5. Airflow via Docker (docker-compose oficial da Apache ou Astro CLI)
   pra rodar o fluxo completo orquestrado

## Diagrama de referência

Falta desenhar uma versão visual do diagrama acima (o de texto já
serve de rascunho). Mermaid.js é a opção mais simples pra manter
versionado dentro do próprio README; excalidraw.com serve se quiser
algo mais "desenhado à mão" pra print/apresentação.

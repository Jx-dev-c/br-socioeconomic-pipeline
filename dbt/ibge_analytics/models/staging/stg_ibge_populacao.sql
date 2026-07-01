-- Staging: só limpa e tipa colunas, sem lógica de negócio.
--
-- Municípios sem estimativa publicada pro ano (marcados '...' pelo IBGE)
-- já foram descartados na extração (ver `parse_populacao` em
-- src/extract/ibge_populacao.py) — aqui só chegam valores válidos.

with source as (

    select * from {{ source('raw', 'ibge_populacao') }}

),

renamed as (

    select
        municipio_id,
        ano,
        populacao_estimada

    from source

)

select * from renamed

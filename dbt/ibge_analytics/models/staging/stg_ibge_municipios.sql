-- Staging: só limpa e tipa colunas, sem lógica de negócio.

with source as (

    select * from {{ source('raw', 'ibge_municipios') }}

),

renamed as (

    select
        municipio_id,
        municipio_nome,
        uf_id,
        uf_sigla,
        uf_nome,
        regiao_id,
        regiao_sigla,
        regiao_nome

    from source

)

select * from renamed

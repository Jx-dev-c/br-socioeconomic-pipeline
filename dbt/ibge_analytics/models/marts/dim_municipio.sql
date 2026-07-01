-- Dimensão: um registro por município. Grão: municipio_id.

with stg as (

    select * from {{ ref('stg_ibge_municipios') }}

)

select
    municipio_id,
    municipio_nome,
    uf_id,
    uf_sigla,
    uf_nome,
    regiao_id,
    regiao_sigla,
    regiao_nome

from stg

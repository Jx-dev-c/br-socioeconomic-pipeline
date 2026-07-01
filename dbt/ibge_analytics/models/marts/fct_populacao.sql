-- Fato: população estimada. Grão: 1 linha por município + ano.

with populacao as (

    select * from {{ ref('stg_ibge_populacao') }}

)

select
    municipio_id,
    ano,
    populacao_estimada

from populacao

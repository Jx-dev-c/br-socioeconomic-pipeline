-- Dimensão: um registro por ano presente nos dados de população.
-- Grão: ano. Gerada a partir dos próprios dados em vez de uma lista fixa,
-- pra nunca ficar dessincronizada com o que realmente foi carregado.

with anos as (

    select distinct ano
    from {{ ref('stg_ibge_populacao') }}

)

select
    ano,
    (ano / 10) * 10 as decada

from anos

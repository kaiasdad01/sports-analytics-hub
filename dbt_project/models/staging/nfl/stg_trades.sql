with source as (
    select * from {{ source('nfl_raw', 'trades') }}
),

renamed as (
    select
        trade_id,
        season,
        trade_date,
        gave,
        received,
        pick_season,
        pick_round,
        pick_number,
        conditional,
        pfr_id,
        pfr_name
    from source
)

select * from renamed

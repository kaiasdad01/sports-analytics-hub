with source as (
    select * from {{ source('nfl_raw', 'officials') }}
),

renamed as (
    select
        game_id,
        game_key,
        official_name,
        position,
        jersey_number,
        official_id,
        season,
        season_type,
        week
    from source
)

select * from renamed

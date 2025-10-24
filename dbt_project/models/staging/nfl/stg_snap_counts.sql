with source as (
    select * from {{ source('nfl_raw', 'snap_counts') }}
),

renamed as (
    select
        game_id,
        pfr_game_id,
        season,
        game_type,
        week,
        player,
        pfr_player_id,
        position,
        team,
        opponent,
        offense_snaps,
        offense_pct,
        defense_snaps,
        defense_pct,
        st_snaps,
        st_pct
    from source
)

select * from renamed

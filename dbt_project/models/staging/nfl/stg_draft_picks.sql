with source as (
    select * from {{ source('nfl_raw', 'draft_picks') }}
),

renamed as (
    select
        season,
        round,
        pick,
        team,
        gsis_id,
        pfr_player_id,
        cfb_player_id,
        pfr_player_name,
        hof,
        position,
        category,
        side,
        college,
        age,
        `to`,
        allpro,
        probowls,
        seasons_started,
        w_av,
        car_av,
        dr_av,
        games,
        pass_completions,
        pass_attempts,
        pass_yards,
        pass_tds,
        pass_ints,
        rush_atts,
        rush_yards,
        rush_tds,
        receptions,
        rec_yards,
        rec_tds,
        def_solo_tackles,
        def_ints,
        def_sacks
    from source
)

select * from renamed

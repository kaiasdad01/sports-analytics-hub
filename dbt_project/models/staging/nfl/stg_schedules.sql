with source as (

    select * from {{ source('nfl_raw', 'schedules') }}

),

renamed as (

    select
        game_id,
        season,
        game_type,
        week,
        gameday,
        weekday,
        gametime,
        away_team,
        away_score,
        home_team,
        home_score,
        location,
        result,
        total,
        overtime,
        old_game_id,
        gsis,
        nfl_detail_id,
        pfr,
        pff,
        espn,
        ftn,
        away_rest,
        home_rest,
        away_moneyline,
        home_moneyline,
        spread_line,
        away_spread_odds,
        home_spread_odds,
        total_line,
        under_odds,
        over_odds,
        div_game,
        roof,
        surface,
        temp,
        wind,
        away_qb_id,
        home_qb_id,
        away_qb_name,
        home_qb_name,
        away_coach,
        home_coach,
        referee,
        stadium_id,
        stadium

    from source

)

select * from renamed
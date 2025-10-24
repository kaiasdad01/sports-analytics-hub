with source as (

    select * from {{ source('nfl_raw', 'nextgen_stats') }}

),

renamed as (

    select
        season,
        season_type,
        week,
        player_display_name,
        player_position,
        team_abbr,
        avg_time_to_throw,
        avg_completed_air_yards,
        avg_intended_air_yards,
        avg_air_yards_differential,
        aggressiveness,
        max_completed_air_distance,
        avg_air_yards_to_sticks,
        attempts,
        pass_yards,
        pass_touchdowns,
        interceptions,
        passer_rating,
        completions,
        completion_percentage,
        expected_completion_percentage,
        completion_percentage_above_expectation,
        avg_air_distance,
        max_air_distance,
        player_gsis_id,
        player_first_name,
        player_last_name,
        player_jersey_number,
        player_short_name

    from source

)

select * from renamed
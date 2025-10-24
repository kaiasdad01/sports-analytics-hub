with source as (
    select * from {{ source('nfl_raw', 'ff_opportunity') }}
),

renamed as (
    select
        -- Game and player identifiers
        season,
        posteam,
        week,
        game_id,
        player_id,
        full_name,
        position,

        -- Attempts
        pass_attempt,
        rec_attempt,
        rush_attempt,

        -- Air yards
        pass_air_yards,
        rec_air_yards,

        -- Completions and receptions
        pass_completions,
        receptions,
        pass_completions_exp,
        receptions_exp,

        -- Yards gained
        pass_yards_gained,
        rec_yards_gained,
        rush_yards_gained,
        pass_yards_gained_exp,
        rec_yards_gained_exp,
        rush_yards_gained_exp,

        -- Touchdowns
        pass_touchdown,
        rec_touchdown,
        rush_touchdown,
        pass_touchdown_exp,
        rec_touchdown_exp,
        rush_touchdown_exp,

        -- Two point conversions
        pass_two_point_conv,
        rec_two_point_conv,
        rush_two_point_conv,
        pass_two_point_conv_exp,
        rec_two_point_conv_exp,
        rush_two_point_conv_exp,

        -- First downs
        pass_first_down,
        rec_first_down,
        rush_first_down,
        pass_first_down_exp,
        rec_first_down_exp,
        rush_first_down_exp,

        -- Turnovers
        pass_interception,
        rec_interception,
        pass_interception_exp,
        rec_interception_exp,
        rec_fumble_lost,
        rush_fumble_lost,

        -- Fantasy points expected
        pass_fantasy_points_exp,
        rec_fantasy_points_exp,
        rush_fantasy_points_exp,

        -- Fantasy points actual
        pass_fantasy_points,
        rec_fantasy_points,
        rush_fantasy_points,

        -- Totals
        total_yards_gained,
        total_yards_gained_exp,
        total_touchdown,
        total_touchdown_exp,
        total_first_down,
        total_first_down_exp,
        total_fantasy_points,
        total_fantasy_points_exp,

        -- Differentials (actual - expected)
        pass_completions_diff,
        receptions_diff,
        pass_yards_gained_diff,
        rec_yards_gained_diff,
        rush_yards_gained_diff,
        pass_touchdown_diff,
        rec_touchdown_diff,
        rush_touchdown_diff,
        pass_two_point_conv_diff,
        rec_two_point_conv_diff,
        rush_two_point_conv_diff,
        pass_first_down_diff,
        rec_first_down_diff,
        rush_first_down_diff,
        pass_interception_diff,
        rec_interception_diff,
        pass_fantasy_points_diff,
        rec_fantasy_points_diff,
        rush_fantasy_points_diff,
        total_yards_gained_diff,
        total_touchdown_diff,
        total_first_down_diff,
        total_fantasy_points_diff,

        -- Team-level attempts
        pass_attempt_team,
        rec_attempt_team,
        rush_attempt_team,

        -- Team-level air yards
        pass_air_yards_team,
        rec_air_yards_team,

        -- Team-level completions
        pass_completions_team,
        receptions_team,
        pass_completions_exp_team,
        receptions_exp_team,

        -- Team-level yards gained
        pass_yards_gained_team,
        rec_yards_gained_team,
        rush_yards_gained_team,
        pass_yards_gained_exp_team,
        rec_yards_gained_exp_team,
        rush_yards_gained_exp_team,

        -- Team-level touchdowns
        pass_touchdown_team,
        rec_touchdown_team,
        rush_touchdown_team,
        pass_touchdown_exp_team,
        rec_touchdown_exp_team,
        rush_touchdown_exp_team,

        -- Team-level two point conversions
        pass_two_point_conv_team,
        rec_two_point_conv_team,
        rush_two_point_conv_team,
        pass_two_point_conv_exp_team,
        rec_two_point_conv_exp_team,
        rush_two_point_conv_exp_team,

        -- Team-level first downs
        pass_first_down_team,
        rec_first_down_team,
        rush_first_down_team,
        pass_first_down_exp_team,
        rec_first_down_exp_team,
        rush_first_down_exp_team,

        -- Team-level turnovers
        pass_interception_team,
        rec_interception_team,
        pass_interception_exp_team,
        rec_interception_exp_team,
        rec_fumble_lost_team,
        rush_fumble_lost_team,

        -- Team-level fantasy points expected
        pass_fantasy_points_exp_team,
        rec_fantasy_points_exp_team,
        rush_fantasy_points_exp_team,

        -- Team-level fantasy points actual
        pass_fantasy_points_team,
        rec_fantasy_points_team,
        rush_fantasy_points_team,

        -- Team-level differentials
        pass_completions_diff_team,
        receptions_diff_team,
        pass_yards_gained_diff_team,
        rec_yards_gained_diff_team,
        rush_yards_gained_diff_team,
        pass_touchdown_diff_team,
        rec_touchdown_diff_team,
        rush_touchdown_diff_team,
        pass_two_point_conv_diff_team,
        rec_two_point_conv_diff_team,
        rush_two_point_conv_diff_team,
        pass_first_down_diff_team,
        rec_first_down_diff_team,
        rush_first_down_diff_team,
        pass_interception_diff_team,
        rec_interception_diff_team,
        pass_fantasy_points_diff_team,
        rec_fantasy_points_diff_team,
        rush_fantasy_points_diff_team,

        -- Team-level totals
        total_yards_gained_team,
        total_yards_gained_exp_team,
        total_yards_gained_diff_team,
        total_touchdown_team,
        total_touchdown_exp_team,
        total_touchdown_diff_team,
        total_first_down_team,
        total_first_down_exp_team,
        total_first_down_diff_team,
        total_fantasy_points_team,
        total_fantasy_points_exp_team,
        total_fantasy_points_diff_team
    from source
)

select * from renamed

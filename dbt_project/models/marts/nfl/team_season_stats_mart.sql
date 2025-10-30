with team_game_stats as (
    select * from {{ ref('team_game_performance_mart') }}
),

season_aggregation as (
    select
        -- Team and season identifiers
        team,
        season,
        season_type,
        
        -- Game counts
        count(distinct game_id) as games_played,
        sum(win) as wins,
        count(distinct game_id) - sum(win) as losses,
        sum(points_scored) as total_points_scored,
        sum(points_allowed) as total_points_allowed,
        avg(points_scored) as avg_points_scored,
        avg(points_allowed) as avg_points_allowed,
        
        -- Offense - Basic stats
        sum(total_yards) as total_yards_season,
        sum(passing_yards) as total_passing_yards,
        sum(rushing_yards) as total_rushing_yards,
        avg(total_yards) as avg_total_yards,
        avg(passing_yards) as avg_passing_yards,
        avg(rushing_yards) as avg_rushing_yards,
        sum(pass_attempts) as total_pass_attempts,
        sum(rush_attempts) as total_rush_attempts,
        sum(completions) as total_completions,
        avg(completion_percentage) as avg_completion_percentage,
        
        -- Passing efficiency
        sum(passing_touchdowns) as total_passing_touchdowns,
        avg(passing_touchdowns) as avg_passing_touchdowns,
        avg(yards_per_attempt) as avg_yards_per_attempt,
        avg(yards_per_completion) as avg_yards_per_completion,
        
        -- Rushing efficiency
        sum(rushing_touchdowns) as total_rushing_touchdowns,
        avg(rushing_touchdowns) as avg_rushing_touchdowns,
        avg(yards_per_carry) as avg_yards_per_carry,
        
        -- First downs breakdown
        sum(first_downs) as total_first_downs,
        sum(rushing_first_downs) as total_rushing_first_downs,
        sum(passing_first_downs) as total_passing_first_downs,
        sum(penalty_first_downs) as total_penalty_first_downs,
        avg(first_downs) as avg_first_downs,
        avg(rushing_first_downs) as avg_rushing_first_downs,
        avg(passing_first_downs) as avg_passing_first_downs,
        avg(penalty_first_downs) as avg_penalty_first_downs,
        
        -- Turnovers
        sum(interceptions) as total_interceptions,
        sum(fumbles_lost) as total_fumbles_lost,
        sum(interceptions) + sum(fumbles_lost) as total_turnovers,
        avg(interceptions) as avg_interceptions_per_game,
        avg(fumbles_lost) as avg_fumbles_lost_per_game,
        
        -- Offensive line
        sum(sacks_allowed) as total_sacks_allowed,
        sum(sack_yards_allowed) as total_sack_yards_allowed,
        sum(tackles_for_loss_allowed) as total_tackles_for_loss_allowed,
        avg(sacks_allowed) as avg_sacks_allowed,
        
        -- Scoring
        sum(touchdowns) as total_touchdowns,
        sum(field_goals_made) as total_field_goals_made,
        sum(field_goals_missed) as total_field_goals_missed,
        sum(field_goals_blocked) as total_field_goals_blocked,
        sum(extra_points_made) as total_extra_points_made,
        sum(extra_points_missed) as total_extra_points_missed,
        sum(two_point_conversions) as total_two_point_conversions,
        
        -- Down efficiency
        sum(third_down_conversions) as total_third_down_conversions,
        sum(third_down_attempts) as total_third_down_attempts,
        sum(third_down_conversions) / nullif(sum(third_down_attempts), 0) as season_third_down_conversion_rate,
        sum(fourth_down_conversions) as total_fourth_down_conversions,
        sum(fourth_down_attempts) as total_fourth_down_attempts,
        sum(fourth_down_conversions) / nullif(sum(fourth_down_attempts), 0) as season_fourth_down_conversion_rate,
        
        -- Red zone
        sum(red_zone_touchdowns) as total_red_zone_touchdowns,
        sum(red_zone_plays) as total_red_zone_plays,
        sum(red_zone_touchdowns) / nullif(sum(red_zone_plays), 0) as season_red_zone_td_rate,
        
        -- EPA and success
        sum(total_epa) as total_epa_season,
        avg(avg_epa) as avg_epa_per_game,
        sum(passing_epa) as total_passing_epa,
        sum(rushing_epa) as total_rushing_epa,
        sum(successful_plays) as total_successful_plays,
        sum(total_plays) as total_plays_season,
        sum(successful_plays) / nullif(sum(total_plays), 0) as season_success_rate,
        
        -- Special teams
        sum(punt_return_yards) as total_punt_return_yards,
        sum(kick_return_yards) as total_kick_return_yards,
        sum(punt_return_touchdowns) as total_punt_return_touchdowns,
        sum(kick_return_touchdowns) as total_kick_return_touchdowns,
        
        -- Penalties
        sum(offensive_penalty_count) as total_offensive_penalties,
        sum(offensive_penalty_yards) as total_offensive_penalty_yards,
        avg(offensive_penalty_count) as avg_offensive_penalties_per_game,
        
        -- Defense - Basic stats
        sum(yards_allowed) as total_yards_allowed,
        sum(passing_yards_allowed) as total_passing_yards_allowed,
        sum(rushing_yards_allowed) as total_rushing_yards_allowed,
        avg(yards_allowed) as avg_yards_allowed_per_game,
        avg(passing_yards_allowed) as avg_passing_yards_allowed_per_game,
        avg(rushing_yards_allowed) as avg_rushing_yards_allowed_per_game,
        
        -- Turnovers forced
        sum(turnovers_forced) as total_turnovers_forced,
        sum(interceptions_forced) as total_interceptions_forced,
        sum(fumbles_recovered) as total_fumbles_recovered,
        sum(pick_sixes) as total_pick_sixes,
        
        -- Defensive plays
        sum(sacks) as total_sacks,
        sum(tackles_for_loss) as total_tackles_for_loss,
        sum(safeties) as total_safeties,
        
        -- Down defense
        sum(third_down_conversions_allowed) as total_third_down_conversions_allowed,
        sum(third_down_attempts_against) as total_third_down_attempts_against,
        sum(third_down_conversions_allowed) / nullif(sum(third_down_attempts_against), 0) as season_third_down_conversion_rate_allowed,
        sum(fourth_down_conversions_allowed) as total_fourth_down_conversions_allowed,
        sum(fourth_down_attempts_against) as total_fourth_down_attempts_against,
        sum(fourth_down_conversions_allowed) / nullif(sum(fourth_down_attempts_against), 0) as season_fourth_down_conversion_rate_allowed,
        
        -- Red zone defense
        sum(red_zone_touchdowns_allowed) as total_red_zone_touchdowns_allowed,
        sum(red_zone_plays_faced) as total_red_zone_plays_faced,
        sum(red_zone_touchdowns_allowed) / nullif(sum(red_zone_plays_faced), 0) as season_red_zone_td_rate_allowed,
        
        -- Defensive penalties
        sum(defensive_penalty_count) as total_defensive_penalties,
        sum(defensive_penalty_yards) as total_defensive_penalty_yards,
        avg(defensive_penalty_count) as avg_defensive_penalties_per_game,
        
        -- Team performance metrics
        (sum(points_scored) - sum(points_allowed)) as point_differential,
        (sum(total_yards) - sum(yards_allowed)) as yard_differential,
        (sum(turnovers_forced) - (sum(interceptions) + sum(fumbles_lost))) as turnover_differential,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at
        
    from team_game_stats
    group by 1, 2, 3
)

select * from season_aggregation

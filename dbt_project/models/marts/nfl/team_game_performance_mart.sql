
with pbp as (
    select * from {{ ref('stg_play_by_play') }}
),

schedules as (
    select * from {{ ref('stg_schedules') }}
),

team_offense as (
    select 
        game_id 
        , possession_team as team
        , season
        , season_type
        , week

        -- Basic yardage
        , sum(case when play_type in ('pass', 'run') then yards_gained else 0 end) as total_yards
        , sum(case when play_type = 'pass' then yards_gained else 0 end) as passing_yards
        , sum(case when play_type = 'run' then yards_gained else 0 end) as rushing_yards
        
        -- Attempts and completions
        , sum(case when pass_attempt = 1 then 1 else 0 end) as pass_attempts
        , sum(case when rush_attempt = 1 then 1 else 0 end) as rush_attempts
        , sum(case when complete_pass = 1 then 1 else 0 end) as completions
        
        -- Passing efficiency
        , sum(case when pass_touchdown = 1 then 1 else 0 end) as passing_touchdowns
        , sum(case when play_type = 'pass' then yards_gained else 0 end) / nullif(sum(case when pass_attempt = 1 then 1 else 0 end), 0) as yards_per_attempt
        , sum(case when play_type = 'pass' then yards_gained else 0 end) / nullif(sum(case when complete_pass = 1 then 1 else 0 end), 0) as yards_per_completion
        
        -- Rushing efficiency  
        , sum(case when rush_touchdown = 1 then 1 else 0 end) as rushing_touchdowns
        , sum(case when play_type = 'run' then yards_gained else 0 end) / nullif(sum(case when rush_attempt = 1 then 1 else 0 end), 0) as yards_per_carry
        
        -- First downs breakdown
        , sum(case when first_down = 1 then 1 else 0 end) as first_downs
        , sum(case when first_down_rush = 1 then 1 else 0 end) as rushing_first_downs
        , sum(case when first_down_pass = 1 then 1 else 0 end) as passing_first_downs
        , sum(case when first_down_penalty = 1 then 1 else 0 end) as penalty_first_downs
        
        -- Turnovers
        , sum(case when interception = 1 then 1 else 0 end) as interceptions
        , sum(case when fumble = 1 then 1 else 0 end) as fumbles
        , sum(case when fumble_lost = 1 then 1 else 0 end) as fumbles_lost

        -- Scoring
        , sum(case when touchdown = 1 then 1 else 0 end) as touchdowns
        , sum(case when field_goal_result = 'made' then 1 else 0 end) as field_goals_made
        , sum(case when field_goal_result = 'missed' then 1 else 0 end) as field_goals_missed
        , sum(case when field_goal_result = 'blocked' then 1 else 0 end) as field_goals_blocked
        , sum(case when extra_point_result = 'made' then 1 else 0 end) as extra_points_made
        , sum(case when extra_point_result = 'missed' then 1 else 0 end) as extra_points_missed
        , sum(case when extra_point_result = 'blocked' then 1 else 0 end) as extra_points_blocked
        , sum(case when two_point_conv_result = 'success' then 1 else 0 end) as two_point_conversions
        , sum(case when two_point_conv_result = 'failed' then 1 else 0 end) as two_point_conversions_failed
        , sum(case when two_point_conv_result = 'blocked' then 1 else 0 end) as two_point_conversions_blocked

        -- Down efficiency
        , count(case when down = 3 then 1 end) as third_down_attempts
        , count(case when down = 3 and third_down_converted = 1 then 1 end) as third_down_conversions
        , count(case when down = 4 then 1 end) as fourth_down_attempts
        , count(case when down = 4 and fourth_down_converted = 1 then 1 end) as fourth_down_conversions

        -- Red zone
        , count(case when yardline_100 <= 20 and play_type in ('pass', 'run') then 1 end) as red_zone_plays
        , sum(case when yardline_100 <= 20 and touchdown = 1 then 1 else 0 end) as red_zone_touchdowns
        , sum(case when yardline_100 <= 20 and field_goal_result = 'made' then 1 else 0 end) as red_zone_field_goals_made
        , sum(case when yardline_100 <= 20 and field_goal_result = 'missed' then 1 else 0 end) as red_zone_field_goals_missed
        , sum(case when yardline_100 <= 20 and field_goal_result = 'blocked' then 1 else 0 end) as red_zone_field_goals_blocked

        -- EPA and success
        , sum(epa) as total_epa
        , avg(epa) as avg_epa
        , sum(case when play_type = 'pass' then epa else 0 end) as passing_epa
        , sum(case when play_type = 'run' then epa else 0 end) as rushing_epa
        , sum(case when success = 1 then 1 else 0 end) as successful_plays
        
        -- Correct total plays (only actual plays, not special teams)
        , count(case when play_type in ('pass', 'run') then 1 end) as total_plays

        -- Offensive line
        , sum(case when sack = 1 then 1 else 0 end) as sacks_allowed
        , sum(case when sack = 1 then abs(yards_gained) else 0 end) as sack_yards_allowed
        , sum(case when tackled_for_loss = 1 then 1 else 0 end) as tackles_for_loss_allowed

        -- Special teams
        , count(case when punt_attempt = 1 then 1 end) as punts
        , sum(case when punt_attempt = 1 then abs(yards_gained) else 0 end) as punt_yards
        , sum(case when punt_attempt = 1 then abs(yards_gained) else 0 end) / nullif(count(case when punt_attempt = 1 then 1 end), 0) as avg_punt_yards
        , count(case when punt_returner_player_id is not null then 1 end) as punt_returns
        , sum(case when punt_returner_player_id is not null then return_yards else 0 end) as punt_return_yards
        , sum(case when punt_returner_player_id is not null and return_touchdown = 1 then 1 else 0 end) as punt_return_touchdowns
        , count(case when kickoff_returner_player_id is not null then 1 end) as kick_returns
        , sum(case when kickoff_returner_player_id is not null then return_yards else 0 end) as kick_return_yards
        , sum(case when kickoff_returner_player_id is not null and return_touchdown = 1 then 1 else 0 end) as kick_return_touchdowns

        -- Long plays
        , max(case when play_type = 'run' then yards_gained end) as longest_rush
        , max(case when play_type = 'pass' then yards_gained end) as longest_reception
        , max(case when punt_attempt = 1 then abs(yards_gained) end) as longest_punt
        , max(case when return_yards > 0 then return_yards end) as longest_return

        -- Penalties
        , count(case when penalty = 1 and penalty_team = possession_team then 1 end) as offensive_penalty_count
        , sum(case when penalty = 1 and penalty_team = possession_team then penalty_yards else 0 end) as penalty_yards

       from pbp
       where possession_team is not null
       group by 1, 2, 3, 4, 5
),

team_defense as (
    select 
          game_id
        , defense_team as team
        
        -- Yards allowed
        , sum(case when play_type in ('pass', 'run') then yards_gained else 0 end) as yards_allowed
        , sum(case when play_type = 'pass' then yards_gained else 0 end) as passing_yards_allowed
        , sum(case when play_type = 'run' then yards_gained else 0 end) as rushing_yards_allowed

        -- Forced turnovers
        , sum(case when interception = 1 then 1 else 0 end) as interceptions_forced
        , sum(case when fumble_forced = 1 then 1 else 0 end) as fumbles_forced
        , sum(case when fumble_lost = 1 then 1 else 0 end) as fumbles_recovered
        , sum(case when interception = 1 and return_touchdown = 1 then 1 else 0 end) as pick_sixes

        -- Defensive plays
        , sum(case when sack = 1 then 1 else 0 end) as sacks
        , sum(case when sack = 1 then abs(yards_gained) else 0 end) as sack_yards
        , sum(case when tackled_for_loss = 1 then 1 else 0 end) as tackles_for_loss
        , sum(case when solo_tackle = 1 then 1 else 0 end) as solo_tackles
        , sum(case when safety = 1 then 1 else 0 end) as safeties

        -- Down defense
        , count(case when down = 3 then 1 end) as third_down_attempts_against
        , count(case when down = 3 and third_down_converted = 1 then 1 end) as third_down_conversions_allowed
        , count(case when down = 4 then 1 end) as fourth_down_attempts_against
        , count(case when down = 4 and fourth_down_converted = 1 then 1 end) as fourth_down_conversions_allowed

        -- Red zone defense 
        , count(case when yardline_100 <= 20 and play_type in ('pass', 'run') then 1 end) as red_zone_plays_faced
        , sum(case when yardline_100 <= 20 and touchdown = 1 then 1 else 0 end) as red_zone_touchdowns_allowed
        , sum(case when yardline_100 <= 20 and field_goal_result = 'made' then 1 else 0 end) as red_zone_field_goals_made_allowed
        , sum(case when yardline_100 <= 20 and field_goal_result = 'missed' then 1 else 0 end) as red_zone_field_goals_missed_defense
        , sum(case when yardline_100 <= 20 and field_goal_result = 'blocked' then 1 else 0 end) as red_zone_field_goals_blocked_defense

        -- Defensive plays count
        , count(case when play_type in ('pass', 'run') then 1 end) as defensive_plays

        -- Defensive penalties
        , count(case when penalty = 1 and penalty_team = defense_team then 1 end) as defensive_penalty_count
        , sum(case when penalty = 1 and penalty_team = defense_team then penalty_yards else 0 end) as penalty_yards

from pbp
where defense_team is not null 
group by 1, 2

), 

final as (
    select 
          o.game_id
        , o.team 
        , o.season 
        , o.week 
        , o.season_type 
        , s.gameday 
        , s.gametime 
        , case 
            when s.home_team = o.team then s.away_team 
            else s.home_team 
        end as opponent
        , case when s.home_team = o.team then 'home' else 'away' end as home_away
        , case when s.home_team = o.team then s.home_score else s.away_score end as points_scored
        , case when s.home_team = o.team then s.away_score else s.home_score end as points_allowed
        , s.result 
        , case 
            when s.home_team = o.team and s.result > 0 then 1
            when s.away_team = o.team and s.result < 0 then 1
            else 0 
        end as win
        , s.spread_line
        , s.total_line
        , s.home_moneyline
        , s.away_moneyline
        , s.roof 
        , s.surface 
        , s.temp 
        , s.wind 
        , s.stadium 

        -- Offense - Basic stats
        , o.total_yards
        , o.passing_yards
        , o.rushing_yards
        , o.pass_attempts
        , o.rush_attempts
        , o.completions
        , safe_divide(o.completions, o.pass_attempts) as completion_percentage
        
        -- Passing efficiency
        , o.passing_touchdowns
        , o.yards_per_attempt
        , o.yards_per_completion
        
        -- Rushing efficiency
        , o.rushing_touchdowns
        , o.yards_per_carry
        
        -- First downs breakdown
        , o.first_downs
        , o.rushing_first_downs
        , o.passing_first_downs
        , o.penalty_first_downs
        
        -- Turnovers
        , o.interceptions
        , o.fumbles
        , o.fumbles_lost
        
        -- Offensive line
        , o.sacks_allowed
        , o.sack_yards_allowed
        , o.tackles_for_loss_allowed
        
        -- Scoring
        , o.touchdowns
        , o.field_goals_made
        , o.field_goals_missed
        , o.field_goals_blocked
        , o.extra_points_made
        , o.extra_points_missed
        , o.extra_points_blocked
        , o.two_point_conversions
        , o.two_point_conversions_failed
        , o.two_point_conversions_blocked
        
        -- Down efficiency
        , o.third_down_attempts
        , o.third_down_conversions
        , safe_divide(o.third_down_conversions, o.third_down_attempts) as third_down_conversion_rate
        , o.fourth_down_attempts
        , o.fourth_down_conversions
        , safe_divide(o.fourth_down_conversions, o.fourth_down_attempts) as fourth_down_conversion_rate
        
        -- Red zone
        , o.red_zone_plays
        , o.red_zone_touchdowns
        , o.red_zone_field_goals_made
        , o.red_zone_field_goals_missed
        , o.red_zone_field_goals_blocked
        , safe_divide(o.red_zone_touchdowns, o.red_zone_plays) as red_zone_td_rate
        
        -- EPA and success
        , o.total_epa
        , o.avg_epa
        , o.passing_epa
        , o.rushing_epa
        , o.successful_plays
        , o.total_plays
        , safe_divide(o.successful_plays, o.total_plays) as success_rate
        
        -- Special teams
        , o.punts
        , o.punt_yards
        , o.avg_punt_yards
        , o.punt_returns
        , o.punt_return_yards
        , o.punt_return_touchdowns
        , o.kick_returns
        , o.kick_return_yards
        , o.kick_return_touchdowns
        
        -- Long plays
        , o.longest_rush
        , o.longest_reception
        , o.longest_punt
        , o.longest_return
        
        -- Penalties
        , o.offensive_penalty_count
        , o.penalty_yards as offensive_penalty_yards

        -- Defense - Basic stats
        , d.yards_allowed
        , d.passing_yards_allowed
        , d.rushing_yards_allowed
        
        -- Turnovers forced
        , d.interceptions_forced
        , d.fumbles_forced
        , d.fumbles_recovered
        , (d.interceptions_forced + d.fumbles_recovered) as turnovers_forced
        , d.pick_sixes
        
        -- Defensive plays
        , d.sacks
        , d.sack_yards
        , d.tackles_for_loss
        , d.solo_tackles
        , d.safeties
        
        -- Down defense
        , d.third_down_attempts_against
        , d.third_down_conversions_allowed
        , safe_divide(d.third_down_conversions_allowed, d.third_down_attempts_against) as third_down_conversion_rate_allowed
        , d.fourth_down_attempts_against
        , d.fourth_down_conversions_allowed
        , safe_divide(d.fourth_down_conversions_allowed, d.fourth_down_attempts_against) as fourth_down_conversion_rate_allowed
        
        -- Red zone defense
        , d.red_zone_plays_faced
        , d.red_zone_touchdowns_allowed
        , d.red_zone_field_goals_made_allowed
        , d.red_zone_field_goals_missed_defense
        , d.red_zone_field_goals_blocked_defense
        , safe_divide(d.red_zone_touchdowns_allowed, d.red_zone_plays_faced) as red_zone_td_rate_allowed
        
        -- Defensive plays count
        , d.defensive_plays
        
        -- Defensive penalties
        , d.defensive_penalty_count
        , d.penalty_yards as defensive_penalty_yards

        -- Metadata
        , current_timestamp() as dbt_loaded_at

    from team_offense o
    left join team_defense d
        on o.game_id = d.game_id
        and o.team = d.team
    left join schedules s
        on o.game_id = s.game_id
)

select * from final

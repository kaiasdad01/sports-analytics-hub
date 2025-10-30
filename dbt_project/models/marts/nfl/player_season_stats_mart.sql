-- models/marts/nfl/player_season_stats_mart.sql
with player_game_stats as (
    select * from {{ ref('player_game_performance_mart') }}
),

player_info as (
    select * from {{ ref('stg_players') }}
),

season_aggregation as (
    select
        -- Player identifiers
        player_id,
        player_name,
        player_display_name,
        position,
        position_group,
        headshot_url,
        
        -- Season identifier
        season,
        season_type,
        team,
        
        -- Game counts
        count(distinct game_id) as games_played,
        sum(win) as wins,
        count(distinct game_id) - sum(win) as losses,
        
        -- Passing stats
        sum(completions) as completions,
        sum(attempts) as attempts,
        avg(case when attempts > 0 then completions / attempts else null end) as completion_pct,
        sum(passing_yards) as passing_yards,
        sum(passing_tds) as passing_tds,
        sum(passing_interceptions) as passing_interceptions,
        sum(sacks_suffered) as sacks_suffered,
        sum(sack_yards_lost) as sack_yards_lost,
        sum(passing_air_yards) as passing_air_yards,
        sum(passing_yards_after_catch) as passing_yards_after_catch,
        sum(passing_first_downs) as passing_first_downs,
        avg(passing_epa) as avg_passing_epa,
        avg(passing_cpoe) as avg_passing_cpoe,
        sum(passing_2pt_conversions) as passing_2pt_conversions,
        
        -- Rushing stats
        sum(carries) as carries,
        sum(rushing_yards) as rushing_yards,
        avg(case when carries > 0 then rushing_yards / carries else null end) as yards_per_carry,
        sum(rushing_tds) as rushing_tds,
        sum(rushing_fumbles) as rushing_fumbles,
        sum(rushing_fumbles_lost) as rushing_fumbles_lost,
        sum(rushing_first_downs) as rushing_first_downs,
        avg(rushing_epa) as avg_rushing_epa,
        sum(rushing_2pt_conversions) as rushing_2pt_conversions,
        
        -- Receiving stats
        sum(receptions) as receptions,
        sum(targets) as targets,
        avg(case when targets > 0 then receptions / targets else null end) as catch_rate,
        sum(receiving_yards) as receiving_yards,
        avg(case when receptions > 0 then receiving_yards / receptions else null end) as yards_per_reception,
        sum(receiving_tds) as receiving_tds,
        sum(receiving_fumbles) as receiving_fumbles,
        sum(receiving_fumbles_lost) as receiving_fumbles_lost,
        sum(receiving_air_yards) as receiving_air_yards,
        sum(receiving_yards_after_catch) as receiving_yards_after_catch,
        sum(receiving_first_downs) as receiving_first_downs,
        avg(receiving_epa) as avg_receiving_epa,
        sum(receiving_2pt_conversions) as receiving_2pt_conversions,
        avg(target_share) as avg_target_share,
        avg(air_yards_share) as avg_air_yards_share,
        avg(wopr) as avg_wopr,
        
        -- Defense stats
        sum(def_tackles_solo) as def_tackles_solo,
        sum(def_tackles_with_assist) as def_tackles_with_assist,
        sum(def_tackle_assists) as def_tackle_assists,
        sum(def_tackles_for_loss) as def_tackles_for_loss,
        sum(def_tackles_for_loss_yards) as def_tackles_for_loss_yards,
        sum(def_fumbles_forced) as def_fumbles_forced,
        sum(def_sacks) as def_sacks,
        sum(def_sack_yards) as def_sack_yards,
        sum(def_qb_hits) as def_qb_hits,
        sum(def_interceptions) as def_interceptions,
        sum(def_interception_yards) as def_interception_yards,
        sum(def_pass_defended) as def_pass_defended,
        sum(def_tds) as def_tds,
        sum(def_fumbles) as def_fumbles,
        sum(def_safeties) as def_safeties,
        
        -- Special teams
        sum(punt_returns) as punt_returns,
        sum(punt_return_yards) as punt_return_yards,
        sum(kickoff_returns) as kickoff_returns,
        sum(kickoff_return_yards) as kickoff_return_yards,
        sum(special_teams_tds) as special_teams_tds,
        
        -- Kicking
        sum(fg_made) as fg_made,
        sum(fg_att) as fg_att,
        sum(fg_missed) as fg_missed,
        sum(fg_blocked) as fg_blocked,
        max(fg_long) as fg_long,
        avg(case when fg_att > 0 then fg_made / fg_att else null end) as fg_pct,
        sum(fg_made_0_19) as fg_made_0_19,
        sum(fg_made_20_29) as fg_made_20_29,
        sum(fg_made_30_39) as fg_made_30_39,
        sum(fg_made_40_49) as fg_made_40_49,
        sum(fg_made_50_59) as fg_made_50_59,
        sum(fg_made_60_) as fg_made_60_plus,
        sum(pat_made) as pat_made,
        sum(pat_att) as pat_att,
        sum(pat_missed) as pat_missed,
        sum(pat_blocked) as pat_blocked,
        avg(case when pat_att > 0 then pat_made / pat_att else null end) as pat_pct,
        sum(gwfg_made) as gwfg_made,
        
        -- Fantasy points
        sum(fantasy_points) as fantasy_points,
        sum(fantasy_points_ppr) as fantasy_points_ppr,
        avg(fantasy_points) as fantasy_points_per_game,
        avg(fantasy_points_ppr) as fantasy_points_ppr_per_game,
        
        -- Total production
        sum(total_yards) as total_yards,
        sum(total_tds) as total_tds,
        sum(total_first_downs) as total_first_downs,
        sum(total_epa) as total_epa,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at
        
    from player_game_stats
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
),

final as (
    select
        s.*,
        p.height,
        p.weight,
        p.college_name,
        p.college_conference,
        p.jersey_number,
        p.rookie_season,
        p.years_of_experience,
        p.draft_year,
        p.draft_round,
        p.draft_pick,
        p.draft_team
    from season_aggregation s
    left join player_info p
        on s.player_id = p.gsis_id
)

select * from final

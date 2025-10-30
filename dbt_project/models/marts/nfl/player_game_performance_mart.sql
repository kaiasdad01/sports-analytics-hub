-- models/marts/player_game_performance_mart.sql
with player_stats as (
    select * from {{ ref('stg_player_stats') }}
),

schedules as (
    select * from {{ ref('stg_schedules') }}
),

players as (
    select * from {{ ref('stg_players') }}
),

nextgen_stats as (
    select * from {{ ref('stg_nextgen_stats') }}
),


final as (
    select
        -- Player core info
        ps.player_id,
        ps.player_name,
        ps.player_display_name,
        ps.position,
        ps.position_group,
        ps.headshot_url,
        
        -- Game & season basic info
        s.game_id,
        ps.season,
        ps.week,
        ps.season_type,
        s.gameday,
        s.gametime,
        
        -- Team basic info
        ps.team,
        ps.opponent_team,
        case when s.home_team = ps.team then 'home' else 'away' end as home_away,
        
        -- Game details
        s.result,
        case 
            when s.home_team = ps.team and s.result > 0 then 1
            when s.away_team = ps.team and s.result < 0 then 1
            else 0 
        end as win,
        s.spread_line,
        s.total_line,
        s.roof,
        s.surface,
        s.temp,
        s.wind,
        s.stadium,
        
        -- Passing stats
        ps.completions,
        ps.attempts,
        ps.passing_yards,
        ps.passing_tds,
        ps.passing_interceptions,
        ps.sacks_suffered,
        ps.sack_yards_lost,
        ps.passing_air_yards,
        ps.passing_yards_after_catch,
        ps.passing_first_downs,
        ps.passing_epa,
        ps.passing_cpoe,
        ps.passing_2pt_conversions,
        ps.pacr,
        
        -- Rushing stats
        ps.carries,
        ps.rushing_yards,
        ps.rushing_tds,
        ps.rushing_fumbles,
        ps.rushing_fumbles_lost,
        ps.rushing_first_downs,
        ps.rushing_epa,
        ps.rushing_2pt_conversions,
        
        -- Receiving stats
        ps.receptions,
        ps.targets,
        ps.receiving_yards,
        ps.receiving_tds,
        ps.receiving_fumbles,
        ps.receiving_fumbles_lost,
        ps.receiving_air_yards,
        ps.receiving_yards_after_catch,
        ps.receiving_first_downs,
        ps.receiving_epa,
        ps.receiving_2pt_conversions,
        ps.racr,
        ps.target_share,
        ps.air_yards_share,
        ps.wopr,
        
        -- Defense stats
        ps.def_tackles_solo,
        ps.def_tackles_with_assist,
        ps.def_tackle_assists,
        ps.def_tackles_for_loss,
        ps.def_tackles_for_loss_yards,
        ps.def_fumbles_forced,
        ps.def_sacks,
        ps.def_sack_yards,
        ps.def_qb_hits,
        ps.def_interceptions,
        ps.def_interception_yards,
        ps.def_pass_defended,
        ps.def_tds,
        ps.def_fumbles,
        ps.def_safeties,
        
        -- Special teams
        ps.punt_returns,
        ps.punt_return_yards,
        ps.kickoff_returns,
        ps.kickoff_return_yards,
        ps.special_teams_tds,
        
        -- Kicking
        ps.fg_made,
        ps.fg_att,
        ps.fg_missed,
        ps.fg_blocked,
        ps.fg_long,
        ps.fg_pct,
        ps.fg_made_0_19,
        ps.fg_made_20_29,
        ps.fg_made_30_39,
        ps.fg_made_40_49,
        ps.fg_made_50_59,
        ps.fg_made_60_,
        ps.pat_made,
        ps.pat_att,
        ps.pat_missed,
        ps.pat_blocked,
        ps.pat_pct,
        ps.gwfg_made,
        
        -- Fantasy points
        ps.fantasy_points,
        ps.fantasy_points_ppr,
        
        -- NGS (NextGen Stats)
        ngs.avg_time_to_throw,
        ngs.avg_completed_air_yards,
        ngs.avg_intended_air_yards,
        ngs.avg_air_yards_differential,
        ngs.aggressiveness,
        ngs.max_completed_air_distance,
        ngs.avg_air_yards_to_sticks,
        ngs.completion_percentage_above_expectation,
        
        -- Calculated metrics
        case when ps.attempts > 0 then ps.completions / ps.attempts else null end as completion_pct,
        case when ps.carries > 0 then ps.rushing_yards / ps.carries else null end as yards_per_carry,
        case when ps.receptions > 0 then ps.receiving_yards / ps.receptions else null end as yards_per_reception,
        case when ps.targets > 0 then ps.receptions / ps.targets else null end as catch_rate,
        
        -- Total production
        (coalesce(ps.passing_yards, 0) + coalesce(ps.rushing_yards, 0) + coalesce(ps.receiving_yards, 0)) as total_yards,
        (coalesce(ps.passing_tds, 0) + coalesce(ps.rushing_tds, 0) + coalesce(ps.receiving_tds, 0) + coalesce(ps.special_teams_tds, 0)) as total_tds,
        (coalesce(ps.passing_first_downs, 0) + coalesce(ps.rushing_first_downs, 0) + coalesce(ps.receiving_first_downs, 0)) as total_first_downs,
        (coalesce(ps.passing_epa, 0) + coalesce(ps.rushing_epa, 0) + coalesce(ps.receiving_epa, 0)) as total_epa,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at
        
    from player_stats ps
    left join schedules s
        on ps.season = s.season
        and ps.week = s.week
        and (ps.team = s.home_team or ps.team = s.away_team)
    left join players p
        on CAST(ps.player_id AS STRING) = p.gsis_id
    left join nextgen_stats ngs
        on ps.season = ngs.season
        and ps.week = ngs.week
        and ps.season_type = ngs.season_type
        and ps.player_display_name = ngs.player_display_name
)

select * from final

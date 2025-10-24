with source as (

    select * from {{ source('nfl_raw', 'depth_charts') }}

),

renamed as (

    select
        season,
        club_code,
        week,
        game_type,
        depth_team,
        last_name,
        first_name,
        football_name,
        formation,
        gsis_id,
        jersey_number,
        position,
        elias_id,
        depth_position,
        full_name,
        dt,
        team,
        player_name,
        espn_id,
        pos_grp_id,
        pos_grp,
        pos_id,
        pos_name,
        pos_abb,
        pos_slot,
        pos_rank

    from source

)

select * from renamed
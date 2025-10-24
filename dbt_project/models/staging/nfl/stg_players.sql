with source as (

    select * from {{ source('nfl_raw', 'players') }}

),

renamed as (

    select
        gsis_id,
        display_name,
        common_first_name,
        first_name,
        last_name,
        short_name,
        football_name,
        suffix,
        esb_id,
        nfl_id,
        pfr_id,
        pff_id,
        otc_id,
        espn_id,
        smart_id,
        birth_date,
        position_group,
        position,
        ngs_position_group,
        ngs_position,
        height,
        weight,
        headshot,
        college_name,
        college_conference,
        jersey_number,
        rookie_season,
        last_season,
        latest_team,
        status,
        ngs_status,
        ngs_status_short_description,
        years_of_experience,
        pff_position,
        pff_status,
        draft_year,
        draft_round,
        draft_pick,
        draft_team

    from source

)

select * from renamed
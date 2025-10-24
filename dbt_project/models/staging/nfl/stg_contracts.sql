with source as (
    select * from {{ source('nfl_raw', 'contracts') }}
),

renamed as (
    select
        player,
        position,
        team,
        is_active,
        year_signed,
        years,
        value,
        apy,
        guaranteed,
        apy_cap_pct,
        inflated_value,
        inflated_apy,
        inflated_guaranteed,
        player_page,
        otc_id,
        gsis_id,
        date_of_birth,
        height,
        weight,
        college,
        draft_year,
        draft_round,
        draft_overall,
        draft_team,
        cols
    from source
)

select * from renamed

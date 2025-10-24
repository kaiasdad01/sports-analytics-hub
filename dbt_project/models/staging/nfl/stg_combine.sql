with source as (
    select * from {{ source('nfl_raw', 'combine') }}
),

renamed as (
    select
        season,
        draft_year,
        draft_team,
        draft_round,
        draft_ovr,
        pfr_id,
        cfb_id,
        player_name,
        pos,
        school,
        ht,
        wt,
        forty,
        bench,
        vertical,
        broad_jump,
        cone,
        shuttle
    from source
)

select * from renamed

with source as (
    select * from {{ source('nfl_raw', 'fines') }}
),

renamed as (
    select
          Club as club
        , Player as player_name
        , Quarter as quarter
        , Time as game_time
        , Fine_Category as fine_category
        , Description as description
        , Amount as fine_amount
        , week
        , JSON_EXTRACT_SCALAR(weekSummary, '$["Total Plays"]') as total_plays
        , JSON_EXTRACT_SCALAR(weekSummary, '$["Resulting in Fines"]') as resulting_in_fines
        , JSON_EXTRACT_SCALAR(weekSummary, '$["% of All Plays"]') as pct_of_all_plays
        , scraped_at
        , source_url

    from source 
)

select * from renamed
with source as (
select * from {{ source('nfl_raw', 'rosters') }}
),

final_data as (
    select 
      *
    , current_timestamp() as dbt_loaded_at
    , '{{ run_started_at }}' as dbt_run_started_at
    from source
)

select * from final_data
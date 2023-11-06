{{
    config(
        materialized='view',
        unique_key='host_id'
    )
}}

with

source as (
    select * from {{ ref('listing_snapshot') }}
    where SCRAPED_DATE is not null
),

renamed as (
    select
        LISTING_ID,
        SCRAPED_DATE, 
        HOST_ID,
        coalesce(listing_neighbourhood, 'Unknown') as listing_neighbourhood,
        case 
            when dbt_valid_from::timestamp = (select min(dbt_valid_from::timestamp) from source) then '2021-01-01 00:00:00+00'::timestamp with time zone 
            else TO_TIMESTAMP(dbt_valid_from, 'YYYY-MM-DD HH24:MI:SS+TZ')::timestamp with time zone 
        end as dbt_valid_from,
        null::timestamp as dbt_valid_to
    from source
)

select * from renamed

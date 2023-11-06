


{{
    config(
        materialized='view',
        unique_key='host_id'
    )
}}

with

source as (
    select * from {{ ref('host_snapshot') }}
    where host_since is not null
),

renamed as (
    select
        host_id as host_id,
        LISTING_ID,
        SCRAPED_DATE, 
        HOST_SINCE,
        coalesce(host_neighbourhood, 'Unknown') as host_neighbourhood,
        coalesce(host_is_superhost, 'Unknown') as host_is_superhost,
        coalesce(host_name, 'Unknown') as host_name,
        case 
            when dbt_valid_from::timestamp = (select min(dbt_valid_from::timestamp) from source) then '2021-01-01 00:00:00+00'::timestamp with time zone 
            else TO_TIMESTAMP(dbt_valid_from, 'YYYY-MM-DD HH24:MI:SS+TZ')::timestamp with time zone 
        end as dbt_valid_from,
        null::timestamp as dbt_valid_to
    from source
)

select * from renamed



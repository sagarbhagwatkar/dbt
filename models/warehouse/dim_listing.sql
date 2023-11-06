{{
    config(
        unique_key='host_id'
    )
}}

select * from {{ ref('listing_stg') }}
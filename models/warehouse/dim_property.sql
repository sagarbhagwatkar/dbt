{{
    config(
        unique_key='host_id'
    )
}}

select * from {{ ref('property_stg') }}
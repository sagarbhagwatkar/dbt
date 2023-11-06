{{
    config(
        unique_key='host_id'
    )
}}

select * from {{ ref('fact_stg') }}
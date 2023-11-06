{{
    config(
        unique_key='host_id'
    )
}}

select * from {{ ref('room_stg') }}
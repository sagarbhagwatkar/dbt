{{
    config(
        unique_key='LGA_CODE_2016'
    )
}}

select * from {{ ref('go2_stg') }}
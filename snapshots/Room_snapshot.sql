{% snapshot Room_snapshot %}

{{
    config(
        target_schema='raw',
        materialized='snapshot',
        unique_key='HOST_ID',
        strategy='timestamp',
        updated_at = 'SCRAPED_DATE'
    )
}}

SELECT LISTING_ID, SCRAPED_DATE,HOST_ID , ROOM_TYPE
FROM {{ source('raw', 'listing') }}


{% endsnapshot %}
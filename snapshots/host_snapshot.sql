
{% snapshot host_snapshot %}

{{
    config(
        target_schema='raw',
        materialized='snapshot',
        unique_key='HOST_ID',
        strategy='timestamp',
        updated_at = 'SCRAPED_DATE'
    )
}}

SELECT LISTING_ID, SCRAPED_DATE,HOST_ID ,HOST_NAME, HOST_SINCE, HOST_IS_SUPERHOST, HOST_NEIGHBOURHOOD
FROM {{ source('raw', 'listing') }}

{% endsnapshot %}





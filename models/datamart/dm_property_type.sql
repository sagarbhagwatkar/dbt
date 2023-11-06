WITH MonthlyData AS (
    SELECT
        dp.property_type,
        dr.room_type,
        df.accommodates,
        EXTRACT(MONTH FROM TO_DATE(df.scraped_date, 'YYYY-MM-DD')) || '/' || EXTRACT(YEAR FROM TO_DATE(df.scraped_date, 'YYYY-MM-DD')) AS month_year,
        COUNT(CASE WHEN df.has_availability = 't' THEN df.listing_id END) AS active_listings,
        COUNT(CASE WHEN df.has_availability = 'f' THEN df.listing_id END) AS inactive_listings,
        ROUND(CAST(COUNT(CASE WHEN df.has_availability = 't' THEN df.listing_id END) * 100.0 / COUNT(df.listing_id) AS NUMERIC), 2) AS active_listings_rate,
        COUNT(DISTINCT CASE WHEN df.has_availability = 't' THEN df.host_id END) AS num_distinct_hosts,
        COUNT(CASE WHEN df.host_is_superhost = 't' THEN df.host_id END) AS num_superhosts,
        ROUND(CAST((COUNT(CASE WHEN df.has_availability = 't' THEN df.listing_id END) * 100.0 / NULLIF(COUNT(DISTINCT CASE WHEN df.has_availability = 't' THEN df.host_id END), 0)) AS NUMERIC), 2) AS superhost_rate,
        COUNT(df.listing_id) AS total_listings,
        MIN(CASE WHEN df.has_availability = 't' THEN df.price END) AS min_price,
        MAX(CASE WHEN df.has_availability = 't' THEN df.price END) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN df.has_availability = 't' THEN df.price END) AS median_price,
        AVG(CASE WHEN df.has_availability = 't' THEN df.price END) AS avg_price,
        AVG(CASE WHEN df.has_availability = 't' THEN df.review_scores_rating END) AS avg_review_scores_rating,
        SUM(CASE WHEN df.has_availability = 't' THEN 30 - COALESCE(CAST(df.availability_30 AS INTEGER), 0) ELSE 0 END) AS total_stays,
        AVG(CASE WHEN df.has_availability = 't' THEN (30 - COALESCE(CAST(df.availability_30 AS INTEGER), 0)) * df.price ELSE 0 END) AS avg_estimated_revenue
    FROM
        {{ ref('dim_fact') }} df
    JOIN
        {{ ref('dim_property') }} dp ON df.listing_id  = dp.listing_id
    JOIN
        {{ ref('dim_room') }} dr ON df.listing_id = dr.listing_id
    GROUP BY
        dp.property_type, dr.room_type, df.accommodates, EXTRACT(YEAR FROM TO_DATE(df.scraped_date, 'YYYY-MM-DD')), EXTRACT(MONTH FROM TO_DATE(df.scraped_date, 'YYYY-MM-DD'))
)
SELECT
    md.property_type,
    md.room_type,
    md.accommodates,
    md.month_year,
    md.active_listings,
    md.inactive_listings,
    md.active_listings_rate,
    md.num_distinct_hosts,
    md.num_superhosts,
    md.superhost_rate,
    md.total_listings,
    md.min_price,
    md.max_price,
    md.median_price,
    md.avg_price,
    md.avg_review_scores_rating,
    md.total_stays,
    md.avg_estimated_revenue
FROM
    MonthlyData md
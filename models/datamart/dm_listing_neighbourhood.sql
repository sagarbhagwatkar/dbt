WITH MonthlyData AS (
    SELECT
        dl.listing_neighbourhood,
        EXTRACT(MONTH FROM TO_DATE(dl.scraped_date, 'YYYY-MM-DD')) || '/' || EXTRACT(YEAR FROM TO_DATE(dl.scraped_date, 'YYYY-MM-DD')) AS month_year,
        COUNT(CASE WHEN df.has_availability = 't' THEN df.listing_id END) AS active_listings,
        COUNT(CASE WHEN df.has_availability = 'f' THEN df.listing_id END) AS inactive_listings,
        ROUND(CAST(COUNT(CASE WHEN df.has_availability = 't' THEN df.listing_id END) * 100.0 / COUNT(df.listing_id) AS NUMERIC), 2) AS active_listings_rate,
        COUNT(DISTINCT CASE WHEN df.has_availability = 't' THEN df.host_id END) AS num_distinct_hosts,
        COUNT(CASE WHEN df.host_is_superhost = 't' THEN df.host_id END) AS num_superhosts,
        ROUND(CAST((COUNT(CASE WHEN df.has_availability = 't' THEN df.listing_id END) * 100.0 / COUNT(DISTINCT CASE WHEN df.has_availability = 't' THEN df.host_id END)) AS NUMERIC), 2) AS superhost_rate,
        COUNT(df.listing_id) AS total_listings,
        MIN(CASE WHEN df.has_availability = 't' THEN df.price END) AS min_price,
        MAX(CASE WHEN df.has_availability = 't' THEN df.price END) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN df.has_availability = 't' THEN df.price END) AS median_price,
        AVG(CASE WHEN df.has_availability = 't' THEN df.price END) AS avg_price,
        AVG(CASE WHEN df.has_availability = 't' THEN df.review_scores_rating END) AS avg_review_scores_rating,
        SUM(30 - CASE WHEN df.has_availability = 't' THEN CAST(df.availability_30 AS INTEGER) ELSE 0 END) AS total_stays
        
	    FROM
	        {{ ref('dim_listing') }} dl
	    JOIN
	        {{ ref('dim_fact') }} df ON dl.listing_id = df.listing_id
	    GROUP BY
	        dl.listing_neighbourhood, EXTRACT(YEAR FROM TO_DATE(dl.scraped_date, 'YYYY-MM-DD')), EXTRACT(MONTH FROM TO_DATE(dl.scraped_date, 'YYYY-MM-DD'))
)   
SELECT
    md.listing_neighbourhood,
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
    md.total_stays * md.avg_price AS estimated_revenue_per_active_listing,
    CASE 
        WHEN md.active_listings <> 0 THEN (next_month.active_listings - md.active_listings) * 100.0 / md.active_listings
        ELSE NULL
    END AS percentage_change_active_listing,
    CASE
        WHEN md.inactive_listings <> 0 THEN (next_month.inactive_listings - md.inactive_listings) * 100.0 / md.inactive_listings
        ELSE NULL
    END AS percentage_change_inactive_listing
FROM
    MonthlyData md
LEFT JOIN
    MonthlyData next_month ON md.listing_neighbourhood = next_month.listing_neighbourhood
    AND TO_DATE(md.month_year, 'MM/YYYY') + INTERVAL '1 month' = TO_DATE(next_month.month_year, 'MM/YYYY')
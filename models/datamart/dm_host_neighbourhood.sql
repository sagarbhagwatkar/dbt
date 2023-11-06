WITH TransformedNeighbourhood AS (
    SELECT
        dh.host_id,
        ds.lga_name AS host_neighbourhood_lga
    FROM
        {{ ref('dim_host') }} dh
    JOIN
        {{ ref('dim_suburb') }} ds ON upper(dh.host_neighbourhood) = upper(ds.suburb_name)
),
ActiveListings AS (
    SELECT
        tn.host_neighbourhood_lga,
        tn.host_id,
        EXTRACT(MONTH FROM TO_DATE(df.scraped_date, 'YYYY-MM-DD')) || '/' || EXTRACT(YEAR FROM TO_DATE(df.scraped_date, 'YYYY-MM-DD')) AS month_year,
        df.listing_id,
        30 - CAST(df.availability_30 AS INTEGER) AS num_stays,
        df.price AS listing_price
    FROM
        TransformedNeighbourhood tn
    JOIN
        {{ ref('dim_fact') }} df ON tn.host_id = df.host_id
    WHERE
        df.has_availability = 't'
),
EstimatedRevenue AS (
    SELECT
        host_neighbourhood_lga,
        host_id,
        month_year,
        listing_id,
        num_stays,
        listing_price,
        num_stays * listing_price AS estimated_revenue
    FROM
        ActiveListings
)
-- Calculate total estimated revenue per active listings and total distinct hosts
SELECT
    host_neighbourhood_lga,
    month_year,
    SUM(estimated_revenue) AS total_estimated_revenue,
    COUNT(DISTINCT listing_id) AS total_active_listings,
    COUNT(DISTINCT host_id) AS total_distinct_hosts,
    SUM(estimated_revenue) / COUNT(DISTINCT host_id) AS estimated_revenue_per_host
FROM
    EstimatedRevenue
GROUP BY
    host_neighbourhood_lga, month_year


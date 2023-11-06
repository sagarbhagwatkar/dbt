SELECT 
    listing_id, 
    scraped_date, 
    host_id, 
    accommodates, 
    price, 
    has_availability,
    HOST_IS_SUPERHOST, 
    availability_30, 
    number_of_reviews, 
    COALESCE(review_scores_rating::double precision, 
             (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY review_scores_rating::double precision) 
              FROM raw.listing 
              WHERE review_scores_rating IS NOT NULL)::double precision) AS review_scores_rating,
    COALESCE(review_scores_accuracy::double precision, 
             (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY review_scores_accuracy::double precision) 
              FROM raw.listing 
              WHERE review_scores_accuracy IS NOT NULL)::double precision) AS review_scores_accuracy,
    COALESCE(review_scores_cleanliness::double precision, 
             (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY review_scores_cleanliness::double precision) 
              FROM raw.listing 
              WHERE review_scores_cleanliness IS NOT NULL)::double precision) AS review_scores_cleanliness,
    COALESCE(review_scores_checkin::double precision, 
             (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY review_scores_checkin::double precision) 
              FROM raw.listing 
              WHERE review_scores_checkin IS NOT NULL)::double precision) AS review_scores_checkin,
    COALESCE(review_scores_communication::double precision, 
             (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY review_scores_communication::double precision) 
              FROM raw.listing 
              WHERE review_scores_communication IS NOT NULL)::double precision) AS review_scores_communication,
    COALESCE(review_scores_value::double precision, 
             (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY review_scores_value::double precision) 
              FROM raw.listing 
              WHERE review_scores_value IS NOT NULL)::double precision) AS review_scores_value
FROM 
    raw.listing




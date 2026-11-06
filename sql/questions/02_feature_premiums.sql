-- Q2: median price per m2 with vs without elevator / balcony / parking, per city.
-- Uses v_listing_latest so each flat counts once. FILTER = conditional aggregate.
SELECT c.city_name,
       count(*) AS flats,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2) FILTER (WHERE has_elevator)::numeric)     AS ppm2_with_elevator,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2) FILTER (WHERE NOT has_elevator)::numeric) AS ppm2_without_elevator,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2) FILTER (WHERE has_balcony)::numeric)      AS ppm2_with_balcony,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2) FILTER (WHERE NOT has_balcony)::numeric)  AS ppm2_without_balcony,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2) FILTER (WHERE has_parking)::numeric)      AS ppm2_with_parking,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2) FILTER (WHERE NOT has_parking)::numeric)  AS ppm2_without_parking
FROM dw.v_listing_latest l
JOIN dw.dim_city c ON c.city_key = l.city_key
WHERE (%(city)s::text IS NULL OR l.city_key = %(city)s::text)
GROUP BY c.city_name
ORDER BY flats DESC;
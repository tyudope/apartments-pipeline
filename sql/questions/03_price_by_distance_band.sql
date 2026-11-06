-- Q3: median price per m2 by distance from the city centre, in 1 km bands.
-- width_bucket assigns each flat to a band 1..10; 11 means "10 km and more".
SELECT c.city_name,
       CASE width_bucket(centre_distance_km, 0, 10, 10)
            WHEN 11 THEN '10+ km'
            ELSE (width_bucket(centre_distance_km, 0, 10, 10) - 1) || '-'
                 || width_bucket(centre_distance_km, 0, 10, 10) || ' km'
       END AS distance_band,
       min(centre_distance_km) AS band_from_km,
       count(*) AS flats,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2)::numeric) AS median_ppm2
FROM dw.v_listing_latest l
JOIN dw.dim_city c ON c.city_key = l.city_key
WHERE (%(city)s::text IS NULL OR l.city_key = %(city)s::text)
GROUP BY c.city_name, width_bucket(centre_distance_km, 0, 10, 10)
ORDER BY c.city_name, band_from_km;
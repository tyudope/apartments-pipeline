-- Q1: median price per m2 by city and month, with month-over-month change.
-- LAG is a window function: it reads the previous row in the same city.
WITH monthly AS (
    SELECT c.city_name,
           l.month_key,
           count(*) AS listings,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2) AS median_ppm2
    FROM dw.fact_listing l
    JOIN dw.dim_city c ON c.city_key = l.city_key
    WHERE (%(city)s::text IS NULL OR l.city_key = %(city)s::text)
    GROUP BY c.city_name, l.month_key
)
SELECT city_name,
       month_key,
       listings,
       round(median_ppm2::numeric) AS median_ppm2,
       round((100.0 * (median_ppm2 - LAG(median_ppm2) OVER (PARTITION BY city_name ORDER BY month_key))
                    / LAG(median_ppm2) OVER (PARTITION BY city_name ORDER BY month_key))::numeric, 1) AS mom_change_pct
FROM monthly
ORDER BY city_name, month_key;
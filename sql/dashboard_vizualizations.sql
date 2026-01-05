-- Graf 1: Množstvo tropických dní v lete
SELECT d.year AS year, FLOOR(100 * COUNT(*) / 92) AS percent_of_tropical_days
FROM facts_measurement m
JOIN dim_date d ON m.dateid = d.dateid
JOIN dim_temperature t ON m.temperatureid = t.temperatureid
WHERE d.season = 'summer' AND t.max >= 30
GROUP BY year;

-- Graf 2: Najvyššia priemerná teplota pre každé ročné obdobie
SELECT season, MAX(avg) AS max_per_season FROM facts_measurement f
JOIN dim_temperature t ON f.temperatureid = t.temperatureid
JOIN dim_date d ON f.dateid = d.dateid
GROUP BY season;

-- Graf 3: Posledný jarný mráz
SELECT date, tmin
FROM (
    SELECT 
        DATE_FROM_PARTS(d.year, d.month, d.day) AS date,
        d.year AS year,
        t.min AS tmin,
        ROW_NUMBER() OVER (PARTITION BY d.year ORDER BY d.month DESC, d.day DESC) as rn
    FROM facts_measurement m
    JOIN dim_date d ON m.dateid = d.dateid
    JOIN dim_temperature t ON m.temperatureid = t.temperatureid
    WHERE d.season = 'spring' AND t.min < 0
) AS subquery
WHERE rn = 1
ORDER BY year ASC;

-- Graf 4: Rebríček teplotných rozdielov pre najnižšiu a najvyššiu teplotu v každom ročnom období
WITH season_temp_intervals AS (
    SELECT season, MIN(min) AS min_temp, MAX(max) AS max_temp
    FROM facts_measurement f
    JOIN dim_date d ON f.dateid = d.dateid
    JOIN dim_temperature t ON f.temperatureid = t.temperatureid
    GROUP BY season
)
SELECT *, (max_temp - min_temp) AS temp_diff, row_number() OVER (
    ORDER BY temp_diff
) AS ranking
FROM season_temp_intervals;

-- Graf 5: Korelácia tlaku a teploty v zime
SELECT p.avg AS pressure, AVG(t.avg) AS temperature
FROM facts_measurement m
JOIN dim_date d ON m.dateid = d.dateid
JOIN dim_pressure p ON m.pressureid = p.pressureid
JOIN dim_temperature t ON m.temperatureid = t.temperatureid
WHERE d.season = 'winter'
GROUP BY p.avg
ORDER BY p.avg ASC;

-- Graf 6: Porovnanie priemerných zrážok a vlhkosti
SELECT d.year AS year, AVG(p.total) AS precipitation, AVG(h.avg) AS humidity
FROM facts_measurement m
JOIN dim_date d ON m.dateid = d.dateid
JOIN dim_humidity h ON m.humidityid = h.humidityid
JOIN dim_precipitation p ON m.precipitationid = p.precipitationid
GROUP BY year
ORDER BY humidity ASC;

-- Graf 7: Medardova kvapka
WITH years_with_precipitation_on_medard AS (
    SELECT DISTINCT d.year AS year
    FROM facts_measurement m
    JOIN dim_date d ON m.dateid = d.dateid
    JOIN dim_precipitation p ON m.precipitationid = p.precipitationid
    WHERE d.month = 6 AND d.day = 8 AND p.total > 0
)
SELECT d.year AS year, COUNT(*) AS days_with_precipitation
FROM facts_measurement m
JOIN dim_date d ON m.dateid = d.dateid
JOIN dim_precipitation p ON m.precipitationid = p.precipitationid
WHERE
    d.year IN (SELECT year FROM years_with_precipitation_on_medard)
    AND DAYOFYEAR(DATE_FROM_PARTS(d.year, d.month, d.day)) BETWEEN 159 AND 199
    AND p.total > 0
GROUP BY year;

-- Graf 8: Celková ročná pravdepodobnosť zrážok
WITH measurements AS (
    SELECT
        d.year,
        COUNT(*) AS total_measurements,
        SUM(CASE WHEN p.total = 0 THEN 1 ELSE 0 END) AS zero_precip_measurements,
        SUM(CASE WHEN p.total > 0 THEN 1 ELSE 0 END) AS rain_measurements,
        SUM(CASE WHEN p.total > 0 THEN 1 ELSE 0 END) * 100.0 / count(*) 
            AS precipitation_probability
    FROM facts_measurement f
    JOIN dim_date d
        on f.dateid = d.dateid
    JOIN dim_precipitation p
        ON f.precipitationid = p.precipitationid
    GROUP BY d.year
    ORDER BY d.year
)
SELECT year, precipitation_probability FROM measurements;

-- Graf 9: Priemerné množstvo snehu v zime podľa rokov
SELECT year, AVG(snowfall) AS average_snowfall FROM facts_measurement f
JOIN dim_date d ON f.dateid = d.dateid
JOIN dim_precipitation p ON f.precipitationid = p.precipitationid
WHERE season = 'winter'
GROUP BY year
ORDER BY year;

-- Graf 10: Priemerná medziročná teplota
WITH cte AS (
    SELECT year, ROUND(AVG(t.avg), 2) AS average FROM facts_measurement f
    JOIN dim_date d ON f.dateid = d.dateid
    JOIN dim_temperature t ON f.temperatureid = t.temperatureid
    GROUP BY year
)
SELECT *, ROW_NUMBER() OVER (
    ORDER BY average
) AS ranking FROM cte;

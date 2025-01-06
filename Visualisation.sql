-- Najlepšie hodnotenia jednotlivých žanrov
SELECT g.genre_name AS movie_genre, AVG(r.rating) AS average_rating FROM fact_ratings r JOIN dim_movies m ON r.dim_movieId = m.dim_movieId JOIN movies_genres_bridge mg ON mg.dim_movieId = m.dim_movieid JOIN dim_genres g ON g.dim_genreId = mg.dim_genreId GROUP BY movie_genre ORDER BY average_rating DESC;  

-- Počet hodnotení podľa jednotlivých vekových skupín
SELECT u.age_group AS age_group, COUNT(r.fact_ratingid) AS count FROM fact_ratings r JOIN dim_users u ON u.dim_userid = r.dim_userid GROUP BY age_group ORDER BY count DESC;

-- Najpopularnejšie tagy od použivateľov
SELECT t.tags AS tags, COUNT(r.fact_ratingid) AS count FROM fact_ratings r JOIN dim_tags t ON r.dim_tagid = t.dim_tagid GROUP BY tags ORDER BY count DESC LIMIT 1; 

--Priemer hodnodenia každeho žánru v zavislosti od genderu
SELECT u.gender AS gender, g.genre_name AS genre, AVG(r.rating) AS rating FROM fact_ratings r JOIN dim_users u ON u.dim_userid = r.dim_userid JOIN dim_movies m ON m.dim_movieid = r.dim_movieid JOIN movies_genres_bridge mg ON mg.dim_movieid = m.dim_movieid JOIN dim_genres g ON g.dim_genreid = mg.dim_genreid GROUP BY gender, genre; 

-- Počet hodnotení v zavislosti od mesiaca v roku 2000
SELECT d.month AS month_int, d.monthAsString AS month_name, COUNT(r.fact_ratingid) AS count FROM fact_ratings r JOIN dim_date d ON d.dim_dateid = r.dim_dateid WHERE d.year = 2000 GROUP BY month_name, month_int ORDER BY month_int;

-- Počet hodnodení v zavislosti od času
SELECT t.hour AS hour, COUNT(r.fact_ratingid) AS count FROM fact_ratings r JOIN dim_time t ON r.dim_timeid = t.dim_timeid GROUP BY hour ORDER BY HOUR;



 
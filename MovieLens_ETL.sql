CREATE DATABASE IF NOT EXISTS COBRA_MovieLens;
CREATE SCHEMA IF NOT EXISTS COBRA_MovieLens.staging;
USE SCHEMA COBRA_MovieLens.staging;

CREATE OR REPLACE TABLE users_staging(
    user_id INTEGER NOT NULL PRIMARY KEY,
    age_id INTEGER NOT NULL FOREIGN KEY REFERENCES age_group_staging(age_group_id),
    gender VARCHAR(1) NOT NULL,
    occupation_id INTEGER NOT NULL FOREIGN KEY REFERENCES occupations_staging(occupation_id),
    zip_code VARCHAR(255) NOT NULL
);
CREATE OR REPLACE TABLE age_group_staging(
    age_group_id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR(45) NOT NULL
);
CREATE OR REPLACE TABLE occupations_staging(
    occupation_id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR(45)
);
CREATE OR REPLACE TABLE tags_staging(
    tag_id INTEGER NOT NULL PRIMARY KEY,
    user_id INTEGER NOT NULL FOREIGN KEY REFERENCES users_staging(user_id),
    movie_id INTEGER NOT NULL FOREIGN KEY REFERENCES movies_staging(movie_id),
    tags VARCHAR(4000) NOT NULL,
    created_at DATETIME NOT NULL
);
CREATE OR REPLACE TABLE movies_staging(
    movie_id INTEGER NOT NULL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    release_year VARCHAR(4) NOT NULL
);
CREATE OR REPLACE TABLE ratings_staging(
    rating_id INTEGER NOT NULL PRIMARY KEY,
    user_id INTEGER NOT NULL FOREIGN KEY REFERENCES users_staging(user_id),
    movie_id INTEGER NOT NULL FOREIGN KEY REFERENCES movies_staging(movie_id),
    rating INTEGER NOT NULL, 
    rated_at DATETIME NOT NULL
);
CREATE OR REPLACE TABLE genres_staging(
    genre_id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);
CREATE OR REPLACE TABLE genres_movies_staging(
    id INTEGER NOT NULL PRIMARY KEY,
    movie_id INTEGER NOT NULL FOREIGN KEY REFERENCES movies_staging(movie_id),
    genre_id INTEGER NOT NULL FOREIGN KEY REFERENCES genres_staging(genre_id)
);

CREATE STAGE IF NOT EXISTS temporary_stage;
LIST @temporary_stage;
CREATE OR REPLACE FILE FORMAT CSV_MovieLens_Format
    TYPE = CSV
    COMPRESSION = NONE
    SKIP_HEADER = 1
    FIELD_DELIMITER = '|'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';
    
COPY INTO users_staging FROM @temporary_stage/users.csv FILE_FORMAT = (FORMAT_NAME = CSV_MovieLens_Format) ON_ERROR='CONTINUE';
COPY INTO genres_staging FROM @temporary_stage/genres.csv FILE_FORMAT = (FORMAT_NAME = CSV_MovieLens_Format) ON_ERROR='CONTINUE';
COPY INTO movies_staging FROM @temporary_stage/movies.csv FILE_FORMAT = (FORMAT_NAME = CSV_MovieLens_Format) ON_ERROR='CONTINUE';
COPY INTO genres_movies_staging FROM @temporary_stage/genres_movies.csv FILE_FORMAT = (FORMAT_NAME = CSV_MovieLens_Format) ON_ERROR='CONTINUE';
COPY INTO ratings_staging FROM @temporary_stage/ratings.csv FILE_FORMAT = (FORMAT_NAME = CSV_MovieLens_Format) ON_ERROR='CONTINUE';
COPY INTO tags_staging FROM @temporary_stage/tags.csv FILE_FORMAT = (FORMAT_NAME = CSV_MovieLens_Format) ON_ERROR='CONTINUE';
COPY INTO age_group_staging FROM @temporary_stage/age_group.csv FILE_FORMAT = (FORMAT_NAME = CSV_MovieLens_Format) ON_ERROR='CONTINUE';
COPY INTO occupations_staging FROM @temporary_stage/occupations.csv FILE_FORMAT = (FORMAT_NAME = CSV_MovieLens_Format) ON_ERROR='CONTINUE';

CREATE TABLE dim_users AS
SELECT u.user_id AS dim_userId, a.name AS age_group, u.gender AS gender, u.zip_code AS zip_code, o.name AS occupation FROM users_staging u JOIN age_group_staging a ON u.age_id = a.age_group_id JOIN occupations_staging o ON u.occupation_id = o.occupation_id;

CREATE TABLE dim_movies AS
SELECT movie_id as dim_movieId, title, release_year FROM movies_staging;

CREATE TABLE dim_genres AS
SELECT genre_id AS dim_genreId, name AS genre_name FROM genres_staging;

CREATE TABLE movies_genres_bridge AS
SELECT movie_id AS dim_movieId, genre_id AS dim_genreId FROM genres_movies_staging;

CREATE TABLE dim_tags AS
SELECT tag_id AS dim_tagId, tags, created_at FROM tags_staging;

CREATE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS DATE) DESC) AS dim_dateId, 
    CAST(rated_at AS DATE) AS date,                    
    DATE_PART(day, rated_at) AS day,                   
    DATE_PART(dow, rated_at) + 1 AS dayOfWeek,        
    CASE DATE_PART(dow, rated_at) + 1
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS dayOfWeekAsString,
    DATE_PART(month, rated_at) AS month,              
    CASE DATE_PART(month, rated_at)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'Oktober'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS monthAsString,
    DATE_PART(year, rated_at) AS year,                
    DATE_PART(week, rated_at) AS week,               
    DATE_PART(quarter, rated_at) AS quarter           
FROM ratings_staging
GROUP BY date, day, dayOfWeek, month, year, week, quarter;

CREATE TABLE dim_time AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS TIME) DESC) AS dim_timeID, 
    CAST(rated_at AS TIME) AS time,                
    CAST(TO_CHAR(rated_at, 'HH24') AS INTEGER) AS hour,
    CAST(TO_CHAR(rated_at, 'MI') AS INTEGER) AS minute,
    CAST(TO_CHAR(rated_at, 'SS') AS INTEGER) AS second
FROM ratings_staging
GROUP BY time, hour, minute, second;

CREATE TABLE fact_ratings AS
SELECT r.rating_id AS fact_ratingId, r.rating AS rating, r.rated_at AS datetime, r.movie_id AS dim_movieId, r.user_id AS dim_userId, t.tag_id AS dim_tagId, dd.dim_dateId AS dim_dateId, dt.dim_timeId AS dim_timeId FROM ratings_staging r JOIN users_staging u ON r.user_id = u.user_id JOIN tags_staging t ON t.user_id = u.user_id JOIN movies_staging m ON t.movie_id = m.movie_id JOIN dim_time dt ON dt.time = CAST(r.rated_at AS TIME) JOIN dim_date dd ON dd.date = CAST(r.rated_at AS DATE);

DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS age_group_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS tags_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS movies_staging;
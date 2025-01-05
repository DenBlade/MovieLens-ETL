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
CREATE DATABASE IF NOT EXISTS COBRA_MovieLens;
CREATE SCHEMA IF NOT EXISTS COBRA_MovieLens.staging;
USE SCHEMA COBRA_MovieLens.staging;

CREATE OR REPLACE TABLE users_staging(
    user_id INTEGER NOT NULL PRIMARY KEY,
    age_id INTEGER NOT NULL FOREIGN KEY REFERENCES age_group_staging(age_group_id),
    gender VARCHAR(1) NOT NULL,
    zip_code VARCHAR(255) NOT NULL,
    occupation_id INTEGER NOT NULL FOREIGN KEY REFERENCES occupations_staging(occupation_id)
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

CREATE OR REPLACE STAGE temporary_stage;


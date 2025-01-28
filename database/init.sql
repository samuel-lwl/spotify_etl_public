-- CREATE DATABASE custom_etl; 
-- #NOTE: might be better to use CREATE DATABASE IF NOT EXISTS but postgres does not have it. we comment this out since the database will already
-- be initialised in docker-compose.yaml. leaving this line here may result in an error and cause init.sql to not run successfully.

-- Use fully qualified statements to ensure correct context
CREATE TABLE IF NOT EXISTS custom_etl.public.new_releases (
    album_type                TEXT,
    artists                   TEXT,
    available_markets         TEXT,
    external_urls             TEXT,
    href                      TEXT,
    id                        TEXT PRIMARY KEY, -- #NOTE: this is the album ID, unique so we can use as primary key. doc: https://developer.spotify.com/documentation/web-api/reference/get-new-releases
    images                    TEXT,
    name                      TEXT,
    release_date              DATE,
    release_date_precision    TEXT,
    total_tracks              INT,
    type                      TEXT,
    uri                       TEXT,
    spotify_available_date    DATE
);
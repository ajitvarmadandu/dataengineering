CREATE OR REPLACE SCHEMA sf_learn.spotify_managed_tables;

--Albums

CREATE OR REPLACE STAGE sf_learn.external_stages.aws_stage_spotify_albums
url = 's3://spotify-etl-dandu/transformed/albums'
storage_integration = READ_FROM_AWS
file_format = 'sf_learn.file_formats.std_csv_format';

CREATE OR REPLACE TABLE sf_learn.spotify_managed_tables.SPOTIFY_ALBUMS_S3(
id VARCHAR(100),
name VARCHAR(100),
release_date DATE,
total_tracks INT,
url VARCHAR(250)
);


CREATE OR REPLACE PIPE sf_learn.pipedb.spotify_albums_pipe
AUTO_INGEST = TRUE
AS
COPY INTO sf_learn.spotify_managed_tables.SPOTIFY_ALBUMS_S3
from @sf_learn.external_stages.aws_stage_spotify_albums
file_format=(format_name='sf_learn.file_formats.std_csv_format');

DESC PIPE sf_learn.pipedb.spotify_albums_pipe;

--Artists

CREATE OR REPLACE STAGE sf_learn.external_stages.aws_stage_spotify_artists
url = 's3://spotify-etl-dandu/transformed/artists'
storage_integration = READ_FROM_AWS
file_format = 'sf_learn.file_formats.std_csv_format';


CREATE OR REPLACE TABLE sf_learn.spotify_managed_tables.SPOTIFY_ARTISTS_S3(
artist_id VARCHAR(100),
artist_name VARCHAR(100),
external_url VARCHAR(250)
);

CREATE OR REPLACE PIPE sf_learn.pipedb.spotify_artists_pipe
AUTO_INGEST = TRUE
AS
COPY INTO sf_learn.spotify_managed_tables.SPOTIFY_ARTISTS_S3
from @sf_learn.external_stages.aws_stage_spotify_artists
file_format=(format_name='sf_learn.file_formats.std_csv_format');

DESC PIPE sf_learn.pipedb.spotify_artists_pipe;

--Songs

CREATE OR REPLACE STAGE sf_learn.external_stages.aws_stage_spotify_songs
url = 's3://spotify-etl-dandu/transformed/songs'
storage_integration = READ_FROM_AWS
file_format = 'sf_learn.file_formats.std_csv_format';



CREATE OR REPLACE TABLE sf_learn.spotify_managed_tables.SPOTIFY_SONGS_S3(
song_id VARCHAR(30),
song_name VARCHAR(100),
duration_ms INT,
url VARCHAR(250),
popularity INT,
song_added TIMESTAMP,
album_id VARCHAR(100),
artist_id VARCHAR(100)
);

CREATE OR REPLACE PIPE sf_learn.pipedb.spotify_songs_pipe
AUTO_INGEST = TRUE
AS
COPY INTO sf_learn.spotify_managed_tables.SPOTIFY_SONGS_S3
from @sf_learn.external_stages.aws_stage_spotify_songs
file_format=(format_name='sf_learn.file_formats.std_csv_format');

DESC PIPE sf_learn.pipedb.spotify_songs_pipe;
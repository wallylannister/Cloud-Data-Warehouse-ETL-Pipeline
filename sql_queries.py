import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    userAgent VARCHAR,
    userId INT
);

""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    duration FLOAT,
    year INT
);
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR
);
""")

user_table_create = ("""
    CREATE TABLE users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
);
""")

song_table_create = ("""
    CREATE TABLE song (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INT,
    duration FLOAT
);
""")

artist_table_create = ("""
    CREATE TABLE artist (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
);
""")

time_table_create = ("""
    CREATE TABLE time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);
""")

# STAGING TABLES

DWH_ROLE_ARN = config.get('IAM_ROLE','ARN')

staging_events_copy = ("""
    COPY staging_events
    FROM 's3://udacity-dend/log_data'
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON 's3://udacity-dend/log_json_path.json';
""").format(DWH_ROLE_ARN)

staging_songs_copy = ("""
    COPY staging_songs
    FROM 's3://udacity-dend/song_data'
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON 'auto';
""").format(DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        TIMESTAMP 'epoch' + log.ts/1000 * INTERVAL '1 second' AS start_time,
        log.userId AS user_id,
        log.level AS level,
        song.song_id AS song_id,
        song.artist_id AS artist_id,
        log.sessionId AS session_id,
        log.location AS location,
        log.userAgent AS user_agent
    FROM staging_events log
    JOIN staging_songs song ON log.song = song.title AND log.artist = song.artist_name
    WHERE log.page = 'NextSong' AND log.length = song.duration;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender AS gender,
        level AS level
    FROM staging_events
    WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO song (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id AS song_id,
        title AS title,
        artist_id AS artist_id,
        year AS year,
        duration AS duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artist (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id AS artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + log.ts/1000 * INTERVAL '1 second' AS start_time,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day,
        EXTRACT(week FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        EXTRACT(weekday FROM start_time) AS weekday
    FROM staging_events log
    WHERE log.page = 'NextSong';
""")

# Analytical
most_listened_artists = ("""
    SELECT 
        a.name AS artist_name,
        COUNT(sp.song_id) AS play_count
    FROM 
        songplays sp
    JOIN 
        artist a ON sp.artist_id = a.artist_id
    GROUP BY 
        a.artist_id, a.name
    ORDER BY 
        play_count DESC
    LIMIT 10;
""")
most_listened_by_male_Nov = ("""
    SELECT 
        s.title AS song_title,
        COUNT(sp.songplay_id) AS play_count
    FROM 
        songplays sp
    JOIN 
        users u ON sp.user_id = u.user_id
    JOIN 
        song s ON sp.song_id = s.song_id
    WHERE 
        u.gender = 'M' 
        AND EXTRACT(MONTH FROM sp.start_time) = 11
    GROUP BY 
        s.title
    ORDER BY 
        play_count DESC;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
analytical_queries = [most_listened_artists, most_listened_by_male_Nov]

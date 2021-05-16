import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR(256),
        auth VARCHAR(50),
        first_name VARCHAR(256),
        gender VARCHAR(1),
        item_in_session INTEGER,
        last_name VARCHAR(256),
        length DECIMAL,
        level VARCHAR(10),
        location VARCHAR(256),
        method VARCHAR(5),
        page VARCHAR(50),
        registration BIGINT,
        session_id INTEGER,
        song VARCHAR(256),
        status INTEGER,
        ts timestamp,
        user_agent VARCHAR(256),
        user_id INTEGER);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INTEGER,
        artist_id VARCHAR(50),
        artist_latitude DECIMAL,
        artist_longitude DECIMAL,
        artist_location VARCHAR(256),
        artist_name VARCHAR(256),
        song_id VARCHAR(256),
        title VARCHAR(256),
        duration DECIMAL,
        year INTEGER);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        song_play_id INTEGER IDENTITY (0,1),
        start_time TIMESTAMP NOT NULL,
        user_id INTEGER NOT NULL,
        level VARCHAR(10),
        song_id VARCHAR(256) NOT NULL, 
        artist_id VARCHAR(50) NOT NULL, 
        session_id INTEGER NOT NULL, 
        location VARCHAR(256), 
        user_agent VARCHAR(256));
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER  NOT NULL,
        first_name VARCHAR(256) NOT NULL,
        last_name VARCHAR(256) NOT NULL,
        gender VARCHAR(1),
        level VARCHAR(10))
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song (
        song_id VARCHAR(256)  NOT NULL,
        title VARCHAR(256)  NOT NULL,
        artist_id VARCHAR(50)  NOT NULL,
        year INTEGER,
        duration DECIMAL);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist (
        artist_id VARCHAR(50)  NOT NULL,
        name VARCHAR(256)  NOT NULL,
        location VARCHAR(256),
        latitude DECIMAL,
        longitude DECIMAL);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP,
        hour INTEGER NOT NULL,
        day INTEGER NOT NULL,
        week INTEGER,
        month INTEGER NOT NULL,
        year INTEGER NOT NULL,
        weekday INTEGER);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {} 
    IAM_ROLE {} 
    REGION 'us-west-2'
    FORMAT AS JSON {}
    TIMEFORMAT 'epochmillisecs';
""").format(config['S3']['LOG_DATA'],
            config['IAM_ROLE']['ARN'],
            config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    IAM_ROLE {}
    REGION 'us-west-2'
    FORMAT AS JSON 'auto'
    TIMEFORMAT 'epochmillisecs';
""").format(config['S3']['SONG_DATA'],
            config['IAM_ROLE']['ARN'],
            config['S3']['LOG_JSONPATH'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent)
    SELECT
    e.ts,
    e.user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.session_id,
    e.location,
    e.user_agent
    FROM staging_events e
    INNER JOIN staging_songs s on s.artist_name = e.artist
    WHERE e.user_id IS NOT NULL;
    
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level)
    SELECT distinct
    e.user_id,
    e.first_name,
    e.last_name,
    e.gender,
    e.level 
    FROM staging_events e
    WHERE e.user_id IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO song (
        song_id,
        title,
        artist_id,
        year,
        duration)
    SELECT
    s.song_id,
    s.title,
    s.artist_id,
    s.year,
    s.duration
    FROM staging_songs s;
""")

artist_table_insert = ("""
    INSERT INTO artist (
        artist_id,
        name,
        location,
        latitude,
        longitude)
    SELECT distinct
    s.artist_id,
    s.artist_name,
    s.artist_location,
    s.artist_latitude,
    s.artist_longitude
    FROM staging_songs s;
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday)
    SELECT
    e.ts,
    EXTRACT(HOUR FROM e.ts),
    EXTRACT (DAY FROM e.ts),
    EXTRACT (WEEK FROM e.ts),
    EXTRACT (MONTH FROM e.ts),
    EXTRACT (YEAR FROM e.ts),
    EXTRACT (DAYOFWEEK FROM e.ts)
    FROM staging_events e;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

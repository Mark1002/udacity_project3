import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist        VARCHAR,
        auth          VARCHAR,
        firstName     VARCHAR,
        gender        VARCHAR,
        itemInSession INT,
        lastName      VARCHAR,
        length        DECIMAL,
        level         VARCHAR,
        location      VARCHAR,
        method        VARCHAR,
        page          VARCHAR,
        registration  DECIMAL,
        sessionId     INT,
        song          VARCHAR DISTKEY,
        status        INT,
        ts            TIMESTAMP,
        userAgent     VARCHAR,
        userId        INT
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs          INT,
        artist_id          VARCHAR, 
        artist_latitude    DECIMAL,
        artist_longitude   DECIMAL,
        artist_location    VARCHAR,
        artist_name        VARCHAR,
        song_id            VARCHAR, 
        title              VARCHAR DISTKEY,
        duration           DECIMAL,
        year               INT
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
      songplay_id int IDENTITY(0,1) PRIMARY KEY,
      start_time TIMESTAMP NOT NULL SORTKEY,
      user_id int NOT NULL,
      level varchar NOT NULL,
      song_id varchar,
      artist_id varchar,
      session_id int NOT NULL,
      location varchar NOT NULL, 
      user_agent varchar NOT NULL
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int PRIMARY KEY DISTKEY,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar PRIMARY KEY,
        title varchar,
        artist_id varchar DISTKEY,
        year int,
        duration numeric
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY DISTKEY,
        name varchar,
        location varchar,
        latitude numeric,
        longitude numeric
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY SORTKEY DISTKEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from '{}' 
    credentials 'aws_iam_role={}'
    TIMEFORMAT as 'epochmillisecs'
    BLANKSASNULL EMPTYASNULL
    compupdate off region 'us-west-2'
    format as json '{}';
""").format(
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH']
)

staging_songs_copy = ("""
    copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    BLANKSASNULL EMPTYASNULL
    compupdate off region 'us-west-2'
    format json 'auto';
""").format(
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN']
)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,
           user_id,
             level,
           song_id,
         artist_id,
        session_id,
          location,
        user_agent
    )
    SELECT 
        se.ts as start_time,
        se.userId as user_id,
        se.level as level,
        ss.song_id as song_id,
        ss.artist_id as artist_id,
        se.sessionId as session_id,
        se.location as location,
        se.userAgent as user_agent
    FROM staging_events se JOIN staging_songs ss
    ON se.song=ss.title
    AND se.artist=ss.artist_name
    AND se.length=ss.duration;
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT DISTINCT 
        userid,
        firstname,
        lastname,
        gender,
        level
    FROM staging_events
    WHERE userid IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (
      start_time,
      hour,
      day,
      week,
      month,
      year,
      weekday
    )
    SELECT DISTINCT
      ts,
      EXTRACT(hour FROM ts),
      EXTRACT(day FROM ts),
      EXTRACT(week FROM ts),
      EXTRACT(month FROM ts),
      EXTRACT(year FROM ts),
      EXTRACT(weekday FROM ts)
    FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

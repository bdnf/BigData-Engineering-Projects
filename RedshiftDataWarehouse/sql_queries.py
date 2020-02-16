import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# TABLE NAMES
# you can define names here and reuse later on
# for readability reasons, however, left 'as is' for now
staging_events_table = "staging_events"
staging_songs_table = "staging_songs"
songplay_table = "songplay"
user_table = "user"
song_table = "song"
artist_table = "artists"
time_table = "time"

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS %s" % staging_events_table
staging_songs_table_drop = "DROP TABLE IF EXISTS %s" % staging_songs_table
songplay_table_drop = "DROP TABLE IF EXISTS %s" % songplay_table
user_table_drop = "DROP TABLE IF EXISTS %s" % songplay_table
song_table_drop = "DROP TABLE IF EXISTS %s" % song_table
artist_table_drop = "DROP TABLE IF EXISTS %s" % artist_table
time_table_drop = "DROP TABLE IF EXISTS %s" % time_table

# CREATE TABLES
staging_events_table_create= ("""

    CREATE TABLE staging_events(
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INTEGER,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INTEGER,
        song                VARCHAR,
        status              INTEGER,
        ts                  TIMESTAMP,
        userAgent           VARCHAR,
        userId              INTEGER
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id         INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time          TIMESTAMP distkey sortkey,
        user_id             VARCHAR,
        level               VARCHAR,
        song_id             VARCHAR,
        artist_id           VARCHAR,
        session_id          VARCHAR,
        location            VARCHAR,
        user_agent          VARCHAR
    );
""")

user_table_create = ("""
     CREATE TABLE IF NOT EXISTS users (
        user_id         VARCHAR PRIMARY KEY sortkey,
        first_name      VARCHAR,
        last_name       VARCHAR,
        gender          VARCHAR,
        level           VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
         song_id        VARCHAR PRIMARY KEY distkey sortkey,
         title          VARCHAR,
         artist_id      VARCHAR,
         artist_name    VARCHAR,
         year           INT,
         duration       FLOAT
      );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id        VARCHAR PRIMARY KEY sortkey,
        artist_name      VARCHAR,
        location         VARCHAR,
        latitude         FLOAT,
        longitude        FLOAT);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time      TIMESTAMP   PRIMARY KEY distkey sortkey,
        hour            INT,
        day             INT,
        week            INT,
        month           INT,
        year            INT,
        weekday         VARCHAR(10)
    );
""")

# STAGING TABLES
staging_events_copy = ("""
    COPY {table} from 's3://udacity-dend/log_data'
     CREDENTIALS 'aws_iam_role={role_arn}'
     COMPUPDATE OFF region '{region}' FORMAT AS JSON 's3://udacity-dend/log_json_path.json'
     TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
     TIMEFORMAT as 'epochmillisecs';
""").format(table=staging_events_table,role_arn=config.get("IAM_ROLE", "arn"), region="us-west-2")

staging_songs_copy = ("""
    COPY {table} from 's3://udacity-dend/song_data'
    CREDENTIALS 'aws_iam_role={role_arn}'
    REGION '{region}' FORMAT AS JSON 'auto';
""").format(table=staging_songs_table,role_arn=config.get("IAM_ROLE", "arn"), region="us-west-2")

print(staging_events_copy)
print(staging_songs_copy)

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays
            (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT staging_events.ts,
               staging_events.userid,
               staging_events.level,
               staging_events.song,
               staging_events.artist,
               staging_events.sessionid,
               staging_events.location,
               staging_events.useragent
        FROM staging_events
        JOIN staging_songs
            ON (staging_songs.title = staging_events.song
                AND staging_songs.artist_name = staging_events.artist)
        AND staging_events.page  =  'NextSong'
""")

user_table_insert = ("""
   INSERT INTO users
	(user_id, first_name, last_name, gender, level)
   SELECT DISTINCT staging_events.userid,
      staging_events.firstname,
      staging_events.lastname,
      staging_events.gender,
      staging_events.level
    FROM staging_events
    WHERE staging_events.userid IS NOT NULL
""")

song_table_insert = ("""
   INSERT INTO songs
	(song_id, title, artist_id, artist_name, year, duration)
   SELECT DISTINCT staging_songs.song_id,
      staging_songs.title,
      staging_songs.artist_id,
      staging_songs.artist_name,
      staging_songs.year,
      staging_songs.duration
    FROM staging_songs
    WHERE staging_songs.song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists
     (artist_id, artist_name, location, latitude, longitude)
    SELECT DISTINCT
       staging_songs.artist_id,
       staging_songs.artist_name,
       staging_songs.artist_location,
       staging_songs.artist_latitude,
       staging_songs.artist_longitude
     FROM staging_songs
     WHERE staging_songs.artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time
     (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT ts,
                    EXTRACT(hour from ts),
                    EXTRACT(day from ts),
                    EXTRACT(week from ts),
                    EXTRACT(month from ts),
                    EXTRACT(year from ts),
                    EXTRACT(weekday from ts)
    FROM staging_events
    WHERE ts IS NOT NULL
""")

# CHECK
songplay_table_count = ("""
    SELECT count(*) FROM songplays
""")

user_table_count = ("""
   SELECT count(*) FROM users
""")

song_table_count = ("""
   SELECT count(*) FROM songs
""")

artist_table_count = ("""
    SELECT count(*) FROM artists
""")

time_table_count = ("""
    SELECT count(*) FROM time
""")


# QUERY LISTS
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]

# ETL
copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]

# CHECK
all_tables_count_rows = [user_table_count, song_table_count, artist_table_count, time_table_count, songplay_table_count]

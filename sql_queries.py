import configparser
# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']



#REGION = config.get("GEO","REGION") 

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_event;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES

staging_events_table_create= ("""
        CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar,  
        auth varchar,
        firstName varchar,
        gender char, 
        itemInSession int, 
        lastName varchar, 
        length float, 
        level varchar, 
        location varchar, 
        method varchar, 
        page varchar, 
        registration float, 
        sessionId int, 
        song varchar,
        status int, 
        ts BIGINT, 
        userAgent varchar, 
        userId int
)
""")

staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs int, 
        artist_id varchar, 
        artist_latitude float, 
        artist_longitude float, 
        artist_location varchar, 
        artist_name varchar, 
        song_id varchar, 
        title varchar, 
        duration float, 
        year int 
)          
""")

songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(0,1)   NOT NULL SORTKEY,
        start_time timestamp NOT NULL,
        user_id int ,  
        level  varchar, 
        song_id  varchar ,  
        artist_id  varchar , 
        session_id  varchar,
        location varchar,
        user_agent varchar
)
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
    user_id   INTEGER  NOT NULL SORTKEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY, 
    title varchar, 
    artist_id varchar, 
    year int, 
    duration numeric
)
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY, 
    name TEXT, 
    location TEXT,
    latitude float,
    longitude float
)
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour int, 
    day int, 
    week int,
    month int, 
    year int,
    weekday int
)
""")


# STAGING TABLES
staging_events_copy = ("""  copy staging_events 
                            from {}
                            iam_role {}
                            json {}
                        """).format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)
staging_songs_copy = (""" copy staging_songs
                            from {}
                            iam_role {}
                            json 'auto'
                      """).format(SONG_DATA, IAM_ROLE)


# FINAL TABLES
songplay_table_insert = ("""
   INSERT INTO songplays(start_time,user_id, level, song_id, artist_id, session_id, location, user_agent)
   SELECT DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time, se.userId as user_id,se.level,ss.song_id as song_id,ss.artist_id,se.sessionId,se.location,se.userAgent
   FROM staging_events AS se
   JOIN staging_songs ss
   ON (se.artist = ss.artist_name)
   where se.page = 'NextSong';
""")

user_table_insert = ("""
   INSERT INTO users(user_id,first_name,last_name,gender,level)
   SELECT DISTINCT userId AS user_id,
         firstName AS first_name,
         lastName AS last_name,
         gender AS gender,
         level AS level
   from staging_events 
   where page = 'NextSong'
""")

song_table_insert = ("""
   INSERT INTO songs(song_id,title,artist_id,year,duration)
   SELECT distinct song_id,title,artist_id,year,duration
   from staging_songs
""")

artist_table_insert = ("""
   INSERT INTO artists(artist_id,name,location,latitude,logitude)
   SELECT distinct artist_id,artist_name as name,artist_location as location,artist_longitude as longitude
   from staging_songs
""")

time_table_insert = ("""
   INSERT into time(start_time,hour,day,week,month,year,weekday)
   select DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,  
           extract(hour from ts),
           extract(day from ts),
           extrcat(week from ts),
           extract(month from ts),
           extract(year from ts),
           extract(weekday from ts)
   from staging_events
   where page = 'NextSong'
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

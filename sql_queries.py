import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS log_data_staging"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_data_staging"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS exists time"

# CREATE TABLES

# staging table for log data
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS log_data_staging (
artist text,
auth text,
firstName text,
gender text,
itemInSession int,
lastName text,
length numeric,
level text,
location text,
method text,
page text,
registration text,
sessionId int,
song text,
status int,
ts bigint,
userAgent text,
userid text
);
""")

# staging table for song dataset
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS song_data_staging (
artist_id text,
artist_latitude numeric,
artist_longitude numeric,
artist_location text,
artist_name text,
song_id text,
title text,
duration numeric,
year int
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id int identity (0, 1) not null,
start_time bigint not null distkey sortkey,
user_id text null,
level text not null,
song_id text null,
artist_id text null,
session_id int not null,
location text not null,
user_agent text not null
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id int sortkey,
first_name text not null,
last_name text not null,
gender text not null,
level text not null
) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id text sortkey,
title text not null,
artist_id text not null,
year int not null,
duration numeric not null
) diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id text sortkey,
name text not null,
location text null,
latitude numeric null,
longitude numeric null
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time bigint sortkey,
hour int not null,
day int not null,
week int not null,
month int not null,
year int not null,
weekday int not null
) diststyle all;
""")

# Extraction into staging tables using redshift copy command


staging_events_copy = ("""
COPY log_data_staging
FROM {}
iam_role {}
format json {}
region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'),
            config.get('CLUSTER', 'my_redshift_role_arn'),
            config.get('S3', 'LOG_JSONPATH'))


staging_songs_copy = ("""
COPY song_data_staging
FROM {}
iam_role {}
format json 'auto'
region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'),
            config.get('CLUSTER', 'my_redshift_role_arn'))

# FINAL TABLES

# Copy into final tables from staging

songplay_table_insert = ("""
INSERT INTO songplays
(start_time, user_id, level,
song_id, artist_id, session_id, location, user_agent)
SELECT st.ts, cast(st.userId as integer),
st.level, s.song_id, a.artist_id, st.sessionid,
st.location, st.useragent
FROM log_data_staging st
LEFT OUTER JOIN songs s
ON st.song = s.title AND st.length  = s.duration
LEFT OUTER JOIN artists a
ON st.artist = a.name
WHERE st.userId != ''
AND page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT cast(userId as integer), firstName, lastName, gender, level
FROM log_data_staging
WHERE userId != ''
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM song_data_staging
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name,
location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name,
artist_location, artist_latitude, artist_longitude
FROM song_data_staging
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts,
date_part('hour', dateadd("ms", ts, '1970-01-01')) as hour,
date_part('day', dateadd("ms", ts, '1970-01-01')) as day,
date_part('week', dateadd("ms", ts, '1970-01-01')) as week,
date_part('month', dateadd("ms", ts, '1970-01-01')) as month,
date_part('year', dateadd("ms", ts, '1970-01-01')) as year,
date_part('dow', dateadd("ms", ts, '1970-01-01')) as weekday
FROM log_data_staging
WHERE page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create, staging_songs_table_create,
    songplay_table_create, user_table_create,
    song_table_create, artist_table_create, time_table_create
]

drop_table_queries = [
    staging_events_table_drop, staging_songs_table_drop,
    songplay_table_drop, user_table_drop,
    song_table_drop, artist_table_drop, time_table_drop
]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [
    song_table_insert, artist_table_insert, user_table_insert,
    artist_table_insert, time_table_insert,
    songplay_table_insert
]

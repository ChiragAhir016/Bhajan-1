import json
from datetime import timedelta
import psycopg2
from psycopg2 import sql

# read file from Songs.json as file and store data in spotify_json
with open("Songs.json", 'r') as file:
    spotify_json = json.load(file)

 #Connection to database
connection = psycopg2.connect(
    dbname="Bhajan",
    user="postgres",
    password="root",
    host="localhost",
    port="5432"
)
curr = connection.cursor() # To do
# create function for convert duration time from millisecond to minutes
def duration_min(ms):
    t = timedelta(milliseconds= ms)
    min = int(t.seconds / 60)
    return min

# create function for convert duration time from millisecond to second
def duration_sec(ms):
    t  = timedelta(milliseconds= ms)
    sec = int(t.seconds % 60)
    return sec

#Insert values from the  json data to  playlist table  in database
curr.execute(sql.SQL("""
    INSERT INTO playlist (playlist_name, description, creator_username, creator_email)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (playlist_name, creator_username) DO NOTHING
    """),
    (
        spotify_json['playlist_name'],
        spotify_json['description'],
        spotify_json['creator']['username'],
        spotify_json['creator']['email']
    ))

#Insert values from the  json data to  track table  in database
for track in spotify_json['tracks']:
    curr.execute(
        sql.SQL("SELECT playlist_id FROM playlist WHERE playlist_name = %s AND creator_username = %s"),
        (spotify_json['playlist_name'], spotify_json['creator']['username'])
    )
    result = curr.fetchone()

    playlist_id = result[0] if result else None

    if playlist_id is not None:
        curr.execute(sql.SQL("""
            INSERT INTO tracks (track_name,playlist_id, artist, album_name, album_release_date, duration_min, popularity, explicit_content, duration_sec)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s)
            ON CONFLICT (track_name, artist) DO NOTHING
            """),
            (
                track['track_name'],
                playlist_id,
                track['artist'],
                track['album']['name'],
                track['album']['release_date'],
                duration_min(track['duration_ms']),
                track['popularity'],
                # track['genres'],
                track['explicit_content'],
                duration_sec(track['duration_ms'])
            ))

# Extract genre names from the JSON data
genre_names = []
for track in spotify_json['tracks']:
    genre_names.extend(track['genres'])

# Remove duplicate genre
genre_names = list(set(genre_names))

# Insert genre names into the genre table
for genre_name in genre_names:
    curr.execute(sql.SQL("""
        INSERT INTO genres (genres_name)
        VALUES (%s)
        ON CONFLICT (genres_name) DO NOTHING
         """), (genre_name,))


connection.commit()

# Close the cursor and connection
curr.close()
connection.close()
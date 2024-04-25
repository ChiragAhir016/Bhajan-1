import json
from datetime import timedelta
import psycopg2
from psycopg2 import sql
from jsonschema import validate

try:
    # read file from Songs.json as file and store data in spotify_json
    with open("Songs.json", 'r') as file:
        spotify_json = json.load(file)
except FileNotFoundError:
    print("file Songs.json is not found.")
    exit()

def validate(spotify_json):
    with open(spotify_json) as file:
        try:
            return json.load(file) # put JSON-data to a variable
        except json.decoder.JSONDecodeError:
            print("Invalid JSON") # in case json is invalid
        else:
            print("Valid JSON")

# Connection to database
connection = psycopg2.connect(
    dbname="Bhajan",
    user="postgres",
    password="root",
    host="localhost",
    port="5432"
)
curr = connection.cursor()


# create function for convert duration time from millisecond to minutes
def duration_time(milliseconds):
    # Convert milliseconds to seconds
    total_seconds = milliseconds / 1000

    # Calculate minutes and seconds
    minutes = int(total_seconds // 60)
    seconds = int(total_seconds % 60)

    return f"{minutes}:{seconds}"


genre_names = []
# Insert values from the  json data to  playlist table  in database

for playlist_data in spotify_json:
    try:
        curr.execute(sql.SQL("""
            INSERT INTO playlist (playlist_name, description, creator_username, creator_email)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (playlist_name, creator_username) DO NOTHING
            """),
                     (
                         playlist_data['playlist_name'],
                         playlist_data['description'],
                         playlist_data['creator']['username'],
                         playlist_data['creator']['email']
                     ))
    except psycopg2.Error as e:
        print("Error occur while inserting in playlist table", e)

    # Insert values from the  json data to  track table  in database
    for track in playlist_data['tracks']:
        genres_str = ','.join(track['genres'])
        try:
            curr.execute(
                sql.SQL("SELECT playlist_id FROM playlist WHERE playlist_name = %s AND creator_username = %s"),
                (playlist_data['playlist_name'], playlist_data['creator']['username'])
            )
            result = curr.fetchone()

            playlist_id = result[0] if result else None

            if playlist_id is not None:
                # Insert album data into the album table
                curr.execute(sql.SQL("""
                                          INSERT INTO album (name, release_date)
                                          VALUES (%s, %s)
                                          ON CONFLICT DO NOTHING
                                      """), (
                    track['album']['name'],
                    track['album']['release_date']
                ))
                # Get the album_id
                curr.execute(sql.SQL("""
                                          SELECT album_id FROM album
                                          WHERE name = %s AND release_date = %s
                                      """), (
                    track['album']['name'],
                    track['album']['release_date']
                ))
                album_id = curr.fetchone()[0]

                curr.execute(sql.SQL("""
                            INSERT INTO tracks (track_name,playlist_id, artist, album_name, album_release_date, duration_time, popularity, genres, explicit_content,album_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (track_name, artist) DO NOTHING
                            """),
                             (
                                 track['track_name'],
                                 playlist_id,
                                 track['artist'],
                                 track['album']['name'],
                                 track['album']['release_date'],
                                 duration_time(track['duration_ms']),
                                 track['popularity'],
                                 genres_str,
                                 track['explicit_content'],
                                 album_id
                             ))
                curr.execute(sql.SQL("""
                            SELECT track_id FROM tracks 
                            WHERE track_name = %s AND artist = %s
                        """), (
                    track['track_name'],
                    track['artist']
                ))
                track_id = curr.fetchone()[0]
        except psycopg2.Error as e:
            print("Error occur while inserting data in track table", e)

    # Extract genre names from the JSON data
    for track in playlist_data['tracks']:
        genre_names.extend(track['genres'])

    # Remove duplicate genre
    genre_names = list(set(genre_names))

    # Insert genre names into the genre table
    for genre_name in genre_names:
        try:
            curr.execute(sql.SQL("""
                        INSERT INTO genres (genres_name)
                        VALUES (%s)
                        ON CONFLICT (genres_name) DO NOTHING
                         """), (genre_name,))

            curr.execute(sql.SQL("""
                                    INSERT INTO track_genres (track_id, genres_id)
                                    SELECT %s, genres_id FROM genres
                                    WHERE genres_name = %s
                                """), (
                track_id,
                genre_name
            ))
        except psycopg2.Error as e:
            print("Error occur while inserting data in genres table", e)

connection.commit()

# Close the cursor and connection
curr.close()
connection.close()
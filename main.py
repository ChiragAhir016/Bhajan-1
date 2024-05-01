import json
from datetime import timedelta
import psycopg2
from psycopg2 import sql
import re
from jsonschema import validate, ValidationError
from datetime import datetime

try:
    # read file from Songs.json as file and store data in spotify_json
    with open("Songs.json", 'r') as file:
            spotify_json = json.load(file)
except FileNotFoundError:
    print("file Songs.json is not found.")
    exit()


def validate_email(email):
    # Regular expression pattern for validating email addresses
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

    # Match the pattern with the email address
    if re.match(pattern, email):
        return True
    else:
        return False


for playlist_data in spotify_json:
    email = playlist_data['creator']['email']
    if validate_email(email):
        print("Valid email!")
        valid_email = True
    else:
        print("inValid email")
        valid_email = False


def validate_integer_values(spotify_json):
    if isinstance(spotify_json, int):
        return True
    else:
        return False


def validate_date(release_date):
    # Attempt to parse the date string
    if datetime.strptime(release_date, '%Y-%m-%d'):
        return True
    else:
        return False

def validate_string_values(value):
    if isinstance(value, str):
        return True
    else:
        return False

for playlist_data in spotify_json:
    for track in playlist_data['tracks']:
        if validate_integer_values(track["popularity"]) and validate_integer_values(track["duration_ms"]):
            print("Valid integer Values!")
            valid_integer = True
        else:
            print("inValid integer Values!")
            valid_integer = False

        if validate_date(track['album']['release_date']):
            print("Valid release_date!")
            valid_Date = True
        else:
            print("Invalid release_date!")
            valid_Date = False

        if validate_string_values(track["track_name"]) and validate_string_values(track["artist"]) and \
                validate_string_values(track["album"]["name"]) and validate_string_values(track["genres"]):
            print("valid string!")
            valid_string = True
        else:
            print("Invalid string!")
            valid_string = False

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
if valid_email:
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
else:
    print("INVALID EMAIL ID CHE GURU!")

if valid_integer and valid_string and valid_Date:
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

else:
    print("INVALID DATE, STRING, INTEGER")

    # Extract genre names from the JSON data
    for track in playlist_data['tracks']:
        genre_names.extend(track['genres'])

    # Remove duplicate genre
    genre_names = list(set(genre_names))

    # # Insert genre names into the genre table
    # for genre_name in genre_names:
    #     try:
    #         curr.execute(sql.SQL("""
    #                     INSERT INTO genres (genres_name)
    #                     VALUES (%s)
    #                     ON CONFLICT (genres_name) DO NOTHING
    #                      """), (genre_name,))
    #
    #         curr.execute(sql.SQL("""
    #                                 INSERT INTO track_genres (track_id, genres_id)
    #                                 SELECT %s, genres_id FROM genres
    #                                 WHERE genres_name = %s
    #                             """), (track_id, genre_name))
    #     except psycopg2.Error as e:
    #         print("Error occur while inserting data in genres table", e)

connection.commit()

# Close the cursor and connection
curr.close()
connection.close()

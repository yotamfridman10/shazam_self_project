import psycopg2
from db import init_db
from song_analysis import open_song_wav 
from song_analysis import analyze_new_song

conn = psycopg2.connect(
    dbname="songs_fingerprints_db", 
    user="postgres", 
    password="1234", 
    host="localhost"
)

init_db(conn)

def main():
    analyze_new_song(conn.cursor(), conn, "song1.wav")


if __name__ == "__main__":
    main()  
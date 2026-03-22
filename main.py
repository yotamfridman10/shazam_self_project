import psycopg2
from db import init_db
from song_analysis import analyze_new_song, analyze_query_song

conn = psycopg2.connect(
    dbname="songs_fingerprints_db", 
    user="postgres", 
    password="1234", 
    host="localhost"
)

init_db(conn)

def main():
    analyze_new_song(conn.cursor(), conn, "song1.wav")
    analyze_new_song(conn.cursor(), conn, "song2.wav")
    analyze_new_song(conn.cursor(), conn, "song3.wav")
    analyze_new_song(conn.cursor(), conn, "song4.wav")
    analyze_new_song(conn.cursor(), conn, "song5.wav")

    best_match_song = analyze_query_song("query_song1.wav", conn.cursor())



if __name__ == "__main__":
    main()  
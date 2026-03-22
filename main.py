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
    analyze_new_song(conn.cursor(), conn, "music/song1.wav", "Avicii - Wake Me Up")
    print("Finished analyzing song1.wav")
    analyze_new_song(conn.cursor(), conn, "music/song2.wav", "Coldplay - Paradise")
    print("Finished analyzing song2.wav")
    analyze_new_song(conn.cursor(), conn, "music/song3.wav", "Coldplay - a Sky Full of Stars")
    print("Finished analyzing song3.wav")
    analyze_new_song(conn.cursor(), conn, "music/song4.wav", "David Guetta - Titanium")
    print("Finished analyzing song4.wav")
    analyze_new_song(conn.cursor(), conn, "music/song5.wav", "On erepublic - Counting Stars")
    print("Finished analyzing song5.wav")
    analyze_new_song(conn.cursor(), conn, "music/song6.wav", "Imagine Dragons - Demons")
    print("Finished analyzing song6.wav")
    analyze_new_song(conn.cursor(), conn, "music/song7.wav", "Shawn Mendes - There's Nothing Holdin' Me Back")
    print("Finished analyzing song7.wav")


    #best_match_song = analyze_query_song("query_song1.wav", conn.cursor())
    #if best_match_song:
    #    print(f"Best match for query_song1.wav: {best_match_song}")


if __name__ == "__main__":
    main()  
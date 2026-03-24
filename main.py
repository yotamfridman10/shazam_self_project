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
    #analyze_new_song(conn, "music/song4.wav", "david guetta - titanium")
    #print("Finished analyzing song4.wav")
   



    best_match_song = analyze_query_song("music/query_song3.wav", conn.cursor())
    if best_match_song:
       print(f"Best match for query_song3.wav: {best_match_song}")


if __name__ == "__main__":
    main()  
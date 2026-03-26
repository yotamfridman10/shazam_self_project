import asyncio 
from psycopg2 import connect                             
from os import cpu_count 
from concurrent.futures import ProcessPoolExecutor, as_completed
from db import init_db, is_song_in_db
from song_analysis import analyze_new_song as analyze1
from song_analysis import analyze_query_song as analyze_query1
from song_analysis_unorginal import analyze_new_song as analyze2
from song_analysis_unorginal import analyze_query_song as analyze_query2

DB_CONFIG = dict(
    dbname="songs_fingerprints_db",
    user="postgres",
    password="1234",
    host="localhost"
)


def process_song(song_file, song_name, method):
   conn = connect(**DB_CONFIG)
   try:
      if method == 1:
         analyze1(conn, song_file, song_name)
      else:
         analyze2(conn, song_file, song_name)
      print(f"Finished successfully analyze{method} of the song: {song_name}")
   except Exception as e:
      conn.rollback() 
      print(f"Error: {e}")
   finally:
      conn.close()


def run_parallel_storage(songs):
   if len(songs) == 0:
      return
   
   with ProcessPoolExecutor(max_workers = cpu_count() - 2) as executor:
      futures = []

      for song_file, song_name in songs:
         futures.append(executor.submit(process_song, song_file, song_name, 1))
         futures.append(executor.submit(process_song, song_file, song_name, 2))

      for f in as_completed(futures):
         try:
            f.result()
         except Exception as e:
            print(f"Error in process: {e}")
      

def query_song(song_file, method):
   conn = connect(**DB_CONFIG)
   try:
      if method == 1:
         best_match_song = analyze_query1(song_file, conn)
      else:
         best_match_song = analyze_query2(song_file, conn) 
      return song_file, best_match_song
   except Exception as e:
      print(f"Error processing {song_file}: {e}")
      return song_file, None
   finally:
      conn.close()
      

async def query_song_async(song_file, method):
   song_file, result = await asyncio.to_thread(query_song, song_file, method)
   if result:
      print(f"Best match for {song_file} by analyze_query2 is: {result}")
   else:
      print(f"No match for {song_file}")


async def run_queries(song_files, method=2):
   tasks = [query_song_async(song, method) for song in song_files]
   await asyncio.gather(*tasks)


def main():

   conn = connect(**DB_CONFIG)

   init_db(conn)

   songs = [
      ("music/song1.wav", "wake me up"),
      ("music/song2.wav", "paradise"),
      ("music/song3.wav", "a sky full of stars"),
      ("music/song4.wav", "titanium"),
      ("music/song5.wav", "counting stars"),
      ("music/song6.wav", "imagine dragons - demons"),
      ("music/song7.wav", "there's nothimg holding me back"),
      ("music/song8.wav", "terminal 3")
   ]

   analyze_songs = [song for song in songs if not is_song_in_db(conn, song[1])]

   conn.close()

   query_songs = [
      "music/query_song1.wav",
      "music/query_song2.wav",
      "music/query_song3.wav",
   ]

   run_parallel_storage(analyze_songs)

   asyncio.run(run_queries(query_songs, method=2))


if __name__ == "__main__":
   main()  
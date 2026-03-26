from csv import writer as csv_writer
from io import StringIO

def init_db(conn, reset_db_flag=False):
    with conn.cursor() as cur:
        if reset_db_flag:
            cur.execute("DROP TABLE IF EXISTS fingerprints;")
            cur.execute("DROP TABLE IF EXISTS windows;")
        cur.execute("CREATE TABLE IF NOT EXISTS fingerprints (hash VARCHAR(40) NOT NULL, song_id TEXT NOT NULL, offset_time FLOAT NOT NULL);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_hash ON fingerprints USING HASH (hash);") 
        cur.execute("CREATE TABLE IF NOT EXISTS windows (id SERIAL PRIMARY KEY, song_name TEXT NOT NULL, relative_seq INTEGER[]);")
        conn.commit()
        print("Database initialized successfully.")


def insert_fingerprint(cursor, song_hash, song_name, offset):
    query = "INSERT INTO fingerprints (hash, song_id, offset_time) VALUES (%s, %s, %s)"
    cursor.execute(query, (song_hash, song_name, offset))


def insert_many_fingerprints(cursor, fingerprints_list):
    query = "INSERT INTO fingerprints (hash, song_id, offset_time) VALUES (%s, %s, %s)"
    cursor.executemany(query, fingerprints_list)


def insert_many_fingerprints_copy(conn, fingerprints_list):
    if not fingerprints_list:
        return

    csv_buffer = StringIO()
    for h, song_name, offset in fingerprints_list:
        csv_buffer.write(f"{h},{song_name},{offset}\n")
    csv_buffer.seek(0)

    with conn.cursor() as cur:
        cur.copy_expert("COPY fingerprints(hash, song_id, offset_time) FROM STDIN WITH CSV",csv_buffer)
    conn.commit()


def get_matches_for_hashes(hashes, cur):
    placeholders = ','.join(['%s'] * len(hashes))
    query = f"SELECT hash, song_id, offset_time FROM fingerprints WHERE hash IN ({placeholders})"
    cur.execute(query, tuple(hashes))
    results = cur.fetchall()
    return results


def insert_many_windows(conn, windows_list):
    import io

    if not windows_list:
        return

    csv_buffer = io.StringIO()
    writer = csv_writer(csv_buffer)

    for song_name, window in windows_list:
        postgres_array = "{" + ",".join(map(str, window)) + "}"
        writer.writerow([song_name, postgres_array])
    csv_buffer.seek(0)

    with conn.cursor() as cur:
        cur.copy_expert("COPY windows(song_name, relative_seq) FROM STDIN WITH CSV",csv_buffer)
    conn.commit()


def get_all_windows(cur):
    cur.execute("SELECT * FROM windows;")
    rows = cur.fetchall()
    return rows


def get_all_windows_by_song(conn, batch_size=10000):
    song_windows = []
    last_song = None

    with conn.cursor("windows_stream_cursor") as cur:
        cur.itersize = batch_size  
        cur.execute("SELECT song_name, relative_seq FROM windows ORDER BY id")

        for row in cur:
            song_name, relative_seq = row
            if last_song is None:
                last_song = song_name

            if song_name != last_song:
                yield last_song, song_windows
                song_windows = []

            song_windows.append((relative_seq))
            last_song = song_name

        if song_windows:
            yield last_song, song_windows


def is_song_in_db(conn, song_name):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM windows WHERE song_name = %s LIMIT 1", (song_name,))
        return cur.fetchone() is not None
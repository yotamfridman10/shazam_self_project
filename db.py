def init_db(conn):
    with conn.cursor() as cur:
        #cur.execute("DROP TABLE IF EXISTS fingerprints;")
        cur.execute("CREATE TABLE IF NOT EXISTS fingerprints (hash VARCHAR(40) NOT NULL, song_id TEXT NOT NULL, offset_time FLOAT NOT NULL);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_hash ON fingerprints USING HASH (hash);") 
        conn.commit()
        print("Database initialized successfully.")


def insert_fingerprint(cursor, song_hash, song_name, offset):
    query = "INSERT INTO fingerprints (hash, song_id, offset_time) VALUES (%s, %s, %s)"
    cursor.execute(query, (song_hash, song_name, offset))

def insert_many_fingerprints(cursor, fingerprints_list):
    query = "INSERT INTO fingerprints (hash, song_id, offset_time) VALUES (%s, %s, %s)"
    cursor.executemany(query, fingerprints_list)

def insert_many_fingerprints_copy(conn, fingerprints_list):
    import io

    if not fingerprints_list:
        return

    csv_buffer = io.StringIO()
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
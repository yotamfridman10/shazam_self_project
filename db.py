def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS fingerprints (hash CHAR(10) NOT NULL, song_id TEXT NOT NULL, offset_time FLOAT NOT NULL);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_hash ON fingerprints USING HASH (hash);") 
        conn.commit()
        print("Database initialized successfully.")


def insert_fingerprint(cursor, song_hash, song_name, offset):
    query = "INSERT INTO fingerprints (hash, song_id, offset_time) VALUES (%s, %s, %s)"
    cursor.execute(query, (song_hash, song_name, offset))

def insert_many_fingerprints(cursor, fingerprints_list):
    query = "INSERT INTO fingerprints (hash, song_id, offset_time) VALUES (%s, %s, %s)"
    cursor.executemany(query, fingerprints_list)
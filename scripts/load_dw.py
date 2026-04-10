import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine(
    "mysql+pymysql://etl_user:etl1234@localhost:3307/grammys_db"
)

# ── Load merged dataset ────────────────────────────────────
df = pd.read_csv("data/merged_dataset.csv")
print("Merged shape:", df.shape)

with engine.connect() as conn:

    # ── dim_date ───────────────────────────────────────────
    years = df['grammy_year'].dropna().unique()
    for year in years:
        conn.execute(text("""
            INSERT IGNORE INTO dim_date (date_id, year)
            VALUES (:date_id, :year)
        """), {"date_id": int(year), "year": int(year)})
    conn.commit()
    print("✅ dim_date loaded")

    # ── dim_artist ─────────────────────────────────────────
    artists = df['artist'].dropna().unique()
    for artist in artists:
        conn.execute(text("""
            INSERT IGNORE INTO dim_artist (artist_name)
            VALUES (:name)
        """), {"name": str(artist)[:500]})
    conn.commit()
    print("✅ dim_artist loaded")

    # ── dim_grammy_category ────────────────────────────────
    categories = df['grammy_category'].dropna().unique()
    for cat in categories:
        conn.execute(text("""
            INSERT IGNORE INTO dim_grammy_category (category_name)
            VALUES (:name)
        """), {"name": str(cat)[:300]})
    conn.commit()
    print("✅ dim_grammy_category loaded")

    # ── dim_song ───────────────────────────────────────────
    songs = df[df['track_id'].notna()][
        ['track_id','song_name','album','track_genre','duration_sec','explicit']
    ].drop_duplicates(subset=['track_id'])

    for _, row in songs.iterrows():
        conn.execute(text("""
            INSERT IGNORE INTO dim_song
                (track_id, song_name, album, track_genre, duration_sec, explicit)
            VALUES
                (:track_id, :song_name, :album, :track_genre, :duration_sec, :explicit)
        """), {
            "track_id":    str(row['track_id'])[:100],
            "song_name":   str(row['song_name'])[:500],
            "album":       str(row['album'])[:500],
            "track_genre": str(row['track_genre'])[:100],
            "duration_sec": float(row['duration_sec']),
            "explicit":    bool(row['explicit'])
        })
    conn.commit()
    print("✅ dim_song loaded")

    # ── fact_grammy_spotify ────────────────────────────────
    inserted = 0
    for _, row in df.iterrows():
        # Get foreign keys
        artist_id = conn.execute(text(
            "SELECT artist_id FROM dim_artist WHERE artist_name = :name"
        ), {"name": str(row['artist'])[:500]}).fetchone()

        category_id = conn.execute(text(
            "SELECT category_id FROM dim_grammy_category WHERE category_name = :name"
        ), {"name": str(row['grammy_category'])[:300]}).fetchone()

        song_id = None
        if pd.notna(row.get('track_id')):
            song_id = conn.execute(text(
                "SELECT song_id FROM dim_song WHERE track_id = :tid"
            ), {"tid": str(row['track_id'])[:100]}).fetchone()

        conn.execute(text("""
            INSERT INTO fact_grammy_spotify (
                artist_id, song_id, category_id, date_id,
                grammy_nominee, grammy_winner, found_in_spotify,
                popularity, danceability, energy, loudness,
                tempo, valence, speechiness, acousticness,
                instrumentalness, liveness
            ) VALUES (
                :artist_id, :song_id, :category_id, :date_id,
                :grammy_nominee, :grammy_winner, :found_in_spotify,
                :popularity, :danceability, :energy, :loudness,
                :tempo, :valence, :speechiness, :acousticness,
                :instrumentalness, :liveness
            )
        """), {
            "artist_id":        artist_id[0] if artist_id else None,
            "song_id":          song_id[0] if song_id else None,
            "category_id":      category_id[0] if category_id else None,
            "date_id":          int(row['grammy_year']) if pd.notna(row['grammy_year']) else None,
            "grammy_nominee":   str(row['grammy_nominee'])[:500] if pd.notna(row.get('grammy_nominee')) else None,
            "grammy_winner":    bool(row['grammy_winner']),
            "found_in_spotify": bool(row['found_in_spotify']),
            "popularity":       float(row['popularity']) if pd.notna(row.get('popularity')) else None,
            "danceability":     float(row['danceability']) if pd.notna(row.get('danceability')) else None,
            "energy":           float(row['energy']) if pd.notna(row.get('energy')) else None,
            "loudness":         float(row['loudness']) if pd.notna(row.get('loudness')) else None,
            "tempo":            float(row['tempo']) if pd.notna(row.get('tempo')) else None,
            "valence":          float(row['valence']) if pd.notna(row.get('valence')) else None,
            "speechiness":      float(row['speechiness']) if pd.notna(row.get('speechiness')) else None,
            "acousticness":     float(row['acousticness']) if pd.notna(row.get('acousticness')) else None,
            "instrumentalness": float(row['instrumentalness']) if pd.notna(row.get('instrumentalness')) else None,
            "liveness":         float(row['liveness']) if pd.notna(row.get('liveness')) else None,
        })
        inserted += 1
        if inserted % 1000 == 0:
            conn.commit()
            print(f"  → {inserted} rows inserted...")

    conn.commit()
    print(f"✅ fact_grammy_spotify loaded — {inserted} rows total")

print("\n✅ Data Warehouse fully loaded!")
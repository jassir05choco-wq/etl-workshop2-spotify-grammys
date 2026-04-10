from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'etl_spotify_grammys',
    default_args=default_args,
    description='ETL pipeline for Spotify and Grammys datasets',
    schedule_interval=None,
    catchup=False,
)

# ── TASK 1: Extract Spotify ────────────────────────────────
def task_extract_spotify(**context):
    df = pd.read_csv("/opt/airflow/data/spotify_dataset.csv")
    df.to_csv("/opt/airflow/data/raw_spotify.csv", index=False)
    print(f"✅ Spotify extracted: {df.shape}")

# ── TASK 2: Extract Grammys ────────────────────────────────
def task_extract_grammys(**context):
    engine = create_engine(
        "mysql+pymysql://etl_user:etl1234@mysql:3306/grammys_db"
    )
    df = pd.read_sql("SELECT * FROM grammy_awards", con=engine)
    df.to_csv("/opt/airflow/data/raw_grammys.csv", index=False)
    print(f"✅ Grammys extracted: {df.shape}")

# ── TASK 3: Clean Spotify ──────────────────────────────────
def task_clean_spotify(**context):
    df = pd.read_csv("/opt/airflow/data/raw_spotify.csv")
    df = df.copy()
    df = df.drop(columns=['Unnamed: 0'], errors='ignore')
    df = df.dropna(subset=['artists', 'track_name', 'album_name'])
    df = df.drop_duplicates()
    df = df.sort_values('popularity', ascending=False)
    df = df.drop_duplicates(subset=['track_id'], keep='first')
    df['artist_normalized'] = df['artists'].str.lower().str.strip()
    df['duration_sec'] = (df['duration_ms'] / 1000).round(2)
    df = df.rename(columns={
        'artists': 'artist',
        'track_name': 'song_name',
        'album_name': 'album'
    })
    df.to_csv("/opt/airflow/data/clean_spotify.csv", index=False)
    print(f"✅ Spotify cleaned: {df.shape}")

# ── TASK 4: Clean Grammys ──────────────────────────────────
def task_clean_grammys(**context):
    df = pd.read_csv("/opt/airflow/data/raw_grammys.csv")
    df = df.dropna(subset=['artist']).copy()
    df['artist_normalized'] = df['artist'].str.lower().str.strip()
    df = df.drop(columns=['title', 'published_at', 'updated_at',
                           'workers', 'img'], errors='ignore')
    df = df.rename(columns={
        'nominee': 'grammy_nominee',
        'category': 'grammy_category',
        'winner': 'grammy_winner',
        'year': 'grammy_year'
    })
    df.to_csv("/opt/airflow/data/clean_grammys.csv", index=False)
    print(f"✅ Grammys cleaned: {df.shape}")

# ── TASK 5: Merge ──────────────────────────────────────────
def task_merge(**context):
    spotify = pd.read_csv("/opt/airflow/data/clean_spotify.csv")
    grammys = pd.read_csv("/opt/airflow/data/clean_grammys.csv")
    merged = pd.merge(grammys, spotify, on='artist_normalized', how='left')
    merged = merged.drop(columns=['artist_normalized', 'artist_y'], errors='ignore')
    merged = merged.rename(columns={'artist_x': 'artist'})
    merged['found_in_spotify'] = merged['track_id'].notna()
    merged.to_csv("/opt/airflow/data/merged_dataset.csv", index=False)
    print(f"✅ Merged: {merged.shape}")

# ── TASK 6: Load to Google Drive ───────────────────────────
def task_store_gdrive(**context):
    # For now we store locally — GDrive integration in next step
    import shutil
    shutil.copy(
        "/opt/airflow/data/merged_dataset.csv",
        "/opt/airflow/data/merged_dataset_gdrive_ready.csv"
    )
    print("✅ File ready for Google Drive upload")

# ── TASK 7: Load to Data Warehouse ────────────────────────
def task_load_dw(**context):
    df = pd.read_csv("/opt/airflow/data/merged_dataset.csv")
    engine = create_engine(
        "mysql+pymysql://etl_user:etl1234@mysql:3306/grammys_db"
    )
    with engine.connect() as conn:
        # Truncate fact table to avoid duplicates on re-runs
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        conn.execute(text("TRUNCATE TABLE fact_grammy_spotify"))
        conn.execute(text("TRUNCATE TABLE dim_artist"))
        conn.execute(text("TRUNCATE TABLE dim_song"))
        conn.execute(text("TRUNCATE TABLE dim_grammy_category"))
        conn.execute(text("TRUNCATE TABLE dim_date"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        conn.commit()

        # dim_date
        for year in df['grammy_year'].dropna().unique():
            conn.execute(text(
                "INSERT IGNORE INTO dim_date (date_id, year) VALUES (:d, :y)"
            ), {"d": int(year), "y": int(year)})
        conn.commit()

        # dim_artist
        for artist in df['artist'].dropna().unique():
            conn.execute(text(
                "INSERT IGNORE INTO dim_artist (artist_name) VALUES (:n)"
            ), {"n": str(artist)[:500]})
        conn.commit()

        # dim_grammy_category
        for cat in df['grammy_category'].dropna().unique():
            conn.execute(text(
                "INSERT IGNORE INTO dim_grammy_category (category_name) VALUES (:n)"
            ), {"n": str(cat)[:300]})
        conn.commit()

        # dim_song
        songs = df[df['track_id'].notna()][
            ['track_id','song_name','album','track_genre','duration_sec','explicit']
        ].drop_duplicates(subset=['track_id'])
        for _, row in songs.iterrows():
            conn.execute(text("""
                INSERT IGNORE INTO dim_song
                    (track_id, song_name, album, track_genre, duration_sec, explicit)
                VALUES (:tid, :sn, :al, :tg, :ds, :ex)
            """), {
                "tid": str(row['track_id'])[:100],
                "sn":  str(row['song_name'])[:500],
                "al":  str(row['album'])[:500],
                "tg":  str(row['track_genre'])[:100],
                "ds":  float(row['duration_sec']),
                "ex":  bool(row['explicit'])
            })
        conn.commit()

        # fact table
        inserted = 0
        for _, row in df.iterrows():
            a = conn.execute(text(
                "SELECT artist_id FROM dim_artist WHERE artist_name=:n"
            ), {"n": str(row['artist'])[:500]}).fetchone()
            c = conn.execute(text(
                "SELECT category_id FROM dim_grammy_category WHERE category_name=:n"
            ), {"n": str(row['grammy_category'])[:300]}).fetchone()
            s = None
            if pd.notna(row.get('track_id')):
                s = conn.execute(text(
                    "SELECT song_id FROM dim_song WHERE track_id=:t"
                ), {"t": str(row['track_id'])[:100]}).fetchone()
            conn.execute(text("""
                INSERT INTO fact_grammy_spotify (
                    artist_id, song_id, category_id, date_id,
                    grammy_nominee, grammy_winner, found_in_spotify,
                    popularity, danceability, energy, loudness,
                    tempo, valence, speechiness, acousticness,
                    instrumentalness, liveness
                ) VALUES (
                    :a, :s, :c, :d, :gn, :gw, :fs,
                    :pop, :dan, :en, :lou, :tem, :val,
                    :spe, :aco, :ins, :liv
                )
            """), {
                "a":   a[0] if a else None,
                "s":   s[0] if s else None,
                "c":   c[0] if c else None,
                "d":   int(row['grammy_year']) if pd.notna(row['grammy_year']) else None,
                "gn":  str(row['grammy_nominee'])[:500] if pd.notna(row.get('grammy_nominee')) else None,
                "gw":  bool(row['grammy_winner']),
                "fs":  bool(row['found_in_spotify']),
                "pop": float(row['popularity']) if pd.notna(row.get('popularity')) else None,
                "dan": float(row['danceability']) if pd.notna(row.get('danceability')) else None,
                "en":  float(row['energy']) if pd.notna(row.get('energy')) else None,
                "lou": float(row['loudness']) if pd.notna(row.get('loudness')) else None,
                "tem": float(row['tempo']) if pd.notna(row.get('tempo')) else None,
                "val": float(row['valence']) if pd.notna(row.get('valence')) else None,
                "spe": float(row['speechiness']) if pd.notna(row.get('speechiness')) else None,
                "aco": float(row['acousticness']) if pd.notna(row.get('acousticness')) else None,
                "ins": float(row['instrumentalness']) if pd.notna(row.get('instrumentalness')) else None,
                "liv": float(row['liveness']) if pd.notna(row.get('liveness')) else None,
            })
            inserted += 1
            if inserted % 1000 == 0:
                conn.commit()
        conn.commit()
    print(f"✅ DW loaded: {inserted} rows")

# ── DEFINE TASKS ───────────────────────────────────────────
t1 = PythonOperator(task_id='extract_spotify',  python_callable=task_extract_spotify,  dag=dag)
t2 = PythonOperator(task_id='extract_grammys',  python_callable=task_extract_grammys,  dag=dag)
t3 = PythonOperator(task_id='clean_spotify',    python_callable=task_clean_spotify,    dag=dag)
t4 = PythonOperator(task_id='clean_grammys',    python_callable=task_clean_grammys,    dag=dag)
t5 = PythonOperator(task_id='merge',            python_callable=task_merge,            dag=dag)
t6 = PythonOperator(task_id='store_gdrive',     python_callable=task_store_gdrive,     dag=dag)
t7 = PythonOperator(task_id='load_dw',          python_callable=task_load_dw,          dag=dag)

# ── DEPENDENCIES ───────────────────────────────────────────
t1 >> t3
t2 >> t4
[t3, t4] >> t5 >> [t6, t7]
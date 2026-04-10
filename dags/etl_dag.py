from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

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
    import pymysql
    conn = pymysql.connect(
        host="mysql", port=3306,
        user="etl_user", password="etl1234",
        database="grammys_db"
    )
    df = pd.read_sql("SELECT * FROM grammy_awards", con=conn)
    conn.close()
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

# ── TASK 6: Store CSV ──────────────────────────────────────
def task_store_gdrive(**context):
    import os
    src = "/opt/airflow/data/merged_dataset.csv"
    dst = "/opt/airflow/data/merged_dataset_gdrive_ready.csv"
    if os.path.exists(dst):
        os.remove(dst)
    with open(src, 'r') as f_in, open(dst, 'w') as f_out:
        f_out.write(f_in.read())
    print("✅ File ready for Google Drive upload")

# ── TASK 7: Load to Data Warehouse ────────────────────────
def task_load_dw(**context):
    import pymysql
    df = pd.read_csv("/opt/airflow/data/merged_dataset.csv")

    conn = pymysql.connect(
        host="mysql", port=3306,
        user="etl_user", password="etl1234",
        database="grammys_db"
    )
    cursor = conn.cursor()

    cursor.execute("SET FOREIGN_KEY_CHECKS=0")
    cursor.execute("TRUNCATE TABLE fact_grammy_spotify")
    cursor.execute("TRUNCATE TABLE dim_artist")
    cursor.execute("TRUNCATE TABLE dim_song")
    cursor.execute("TRUNCATE TABLE dim_grammy_category")
    cursor.execute("TRUNCATE TABLE dim_date")
    cursor.execute("SET FOREIGN_KEY_CHECKS=1")
    conn.commit()

    for year in df['grammy_year'].dropna().unique():
        cursor.execute("INSERT IGNORE INTO dim_date (date_id, year) VALUES (%s, %s)", (int(year), int(year)))
    conn.commit()

    for artist in df['artist'].dropna().unique():
        cursor.execute("INSERT IGNORE INTO dim_artist (artist_name) VALUES (%s)", (str(artist)[:500],))
    conn.commit()

    for cat in df['grammy_category'].dropna().unique():
        cursor.execute("INSERT IGNORE INTO dim_grammy_category (category_name) VALUES (%s)", (str(cat)[:300],))
    conn.commit()

    songs = df[df['track_id'].notna()][
        ['track_id','song_name','album','track_genre','duration_sec','explicit']
    ].drop_duplicates(subset=['track_id'])
    for _, row in songs.iterrows():
        cursor.execute("""
            INSERT IGNORE INTO dim_song (track_id, song_name, album, track_genre, duration_sec, explicit)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (str(row['track_id'])[:100], str(row['song_name'])[:500],
              str(row['album'])[:500], str(row['track_genre'])[:100],
              float(row['duration_sec']), bool(row['explicit'])))
    conn.commit()

    inserted = 0
    for _, row in df.iterrows():
        cursor.execute("SELECT artist_id FROM dim_artist WHERE artist_name=%s", (str(row['artist'])[:500],))
        a = cursor.fetchone()
        cursor.execute("SELECT category_id FROM dim_grammy_category WHERE category_name=%s", (str(row['grammy_category'])[:300],))
        c = cursor.fetchone()
        s = None
        if pd.notna(row.get('track_id')):
            cursor.execute("SELECT song_id FROM dim_song WHERE track_id=%s", (str(row['track_id'])[:100],))
            s = cursor.fetchone()
        cursor.execute("""
            INSERT INTO fact_grammy_spotify (
                artist_id, song_id, category_id, date_id,
                grammy_nominee, grammy_winner, found_in_spotify,
                popularity, danceability, energy, loudness,
                tempo, valence, speechiness, acousticness,
                instrumentalness, liveness
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            a[0] if a else None, s[0] if s else None,
            c[0] if c else None,
            int(row['grammy_year']) if pd.notna(row['grammy_year']) else None,
            str(row['grammy_nominee'])[:500] if pd.notna(row.get('grammy_nominee')) else None,
            bool(row['grammy_winner']), bool(row['found_in_spotify']),
            float(row['popularity']) if pd.notna(row.get('popularity')) else None,
            float(row['danceability']) if pd.notna(row.get('danceability')) else None,
            float(row['energy']) if pd.notna(row.get('energy')) else None,
            float(row['loudness']) if pd.notna(row.get('loudness')) else None,
            float(row['tempo']) if pd.notna(row.get('tempo')) else None,
            float(row['valence']) if pd.notna(row.get('valence')) else None,
            float(row['speechiness']) if pd.notna(row.get('speechiness')) else None,
            float(row['acousticness']) if pd.notna(row.get('acousticness')) else None,
            float(row['instrumentalness']) if pd.notna(row.get('instrumentalness')) else None,
            float(row['liveness']) if pd.notna(row.get('liveness')) else None,
        ))
        inserted += 1
        if inserted % 1000 == 0:
            conn.commit()

    conn.commit()
    cursor.close()
    conn.close()
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
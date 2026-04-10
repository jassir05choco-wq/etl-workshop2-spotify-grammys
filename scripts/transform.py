import pandas as pd
from sqlalchemy import create_engine

# ── EXTRACT ────────────────────────────────────────────────

def extract_spotify():
    df = pd.read_csv("/opt/airflow/data/spotify_dataset.csv")
    return df

def extract_grammys():
    engine = create_engine(
        "mysql+pymysql://etl_user:etl1234@mysql:3306/grammys_db"
    )
    df = pd.read_sql("SELECT * FROM grammy_awards", con=engine)
    return df

# ── CLEAN SPOTIFY ──────────────────────────────────────────

def clean_spotify(df):
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
    return df

# ── CLEAN GRAMMYS ──────────────────────────────────────────

def clean_grammys(df):
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
    return df

# ── MERGE ──────────────────────────────────────────────────

def merge_datasets(spotify_clean, grammys_clean):
    merged = pd.merge(
        grammys_clean,
        spotify_clean,
        on='artist_normalized',
        how='left'
    )
    merged = merged.drop(columns=['artist_normalized', 'artist_y'], errors='ignore')
    merged = merged.rename(columns={'artist_x': 'artist'})
    merged['found_in_spotify'] = merged['track_id'].notna()
    return merged

# ── MAIN ───────────────────────────────────────────────────

if __name__ == "__main__":
    print("Extracting...")
    spotify_raw = pd.read_csv("data/spotify_dataset.csv")
    grammys_raw = pd.read_csv("data/the_grammy_awards.csv")

    print("Cleaning...")
    spotify_clean = clean_spotify(spotify_raw)
    grammys_clean = clean_grammys(grammys_raw)

    print("Spotify clean shape:", spotify_clean.shape)
    print("Grammys clean shape:", grammys_clean.shape)

    print("Merging...")
    merged = merge_datasets(spotify_clean, grammys_clean)

    print("Merged shape:", merged.shape)
    print("Columns:", merged.columns.tolist())
    print("Found in Spotify:", merged['found_in_spotify'].sum())
    print(merged.head(3))

    merged.to_csv("data/merged_dataset.csv", index=False)
    print("✅ Merged dataset saved to data/merged_dataset.csv")
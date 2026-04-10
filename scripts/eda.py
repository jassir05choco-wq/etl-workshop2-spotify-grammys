import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="whitegrid")

# ── Load datasets ──────────────────────────────────────────
spotify = pd.read_csv("data/spotify_dataset.csv")
grammys = pd.read_csv("data/the_grammy_awards.csv")

# ── SPOTIFY ────────────────────────────────────────────────
print("=" * 50)
print("SPOTIFY DATASET")
print("=" * 50)
print("Shape:", spotify.shape)
print("\nDtypes:\n", spotify.dtypes)
print("\nNulls:\n", spotify.isnull().sum())
print("\nBasic stats:\n", spotify[['popularity','duration_ms','danceability','energy','tempo']].describe())

# Drop unnamed index column
spotify = spotify.drop(columns=['Unnamed: 0'], errors='ignore')

# Check duplicates
print("\nDuplicate rows:", spotify.duplicated().sum())
print("Duplicate track_id:", spotify['track_id'].duplicated().sum())

# ── GRAMMYS ────────────────────────────────────────────────
print("\n" + "=" * 50)
print("GRAMMYS DATASET")
print("=" * 50)
print("Shape:", grammys.shape)
print("\nNulls:\n", grammys.isnull().sum())
print("\nYear range:", grammys['year'].min(), "-", grammys['year'].max())
print("\nTop 10 categories:\n", grammys['category'].value_counts().head(10))
print("\nDuplicate rows:", grammys.duplicated().sum())

# ── OVERLAP ────────────────────────────────────────────────
print("\n" + "=" * 50)
print("ARTIST OVERLAP")
print("=" * 50)
spotify_artists = set(spotify['artists'].dropna().str.lower().str.strip())
grammy_artists  = set(grammys['artist'].dropna().str.lower().str.strip())
overlap = spotify_artists & grammy_artists
print(f"Spotify unique artists : {len(spotify_artists)}")
print(f"Grammy unique artists  : {len(grammy_artists)}")
print(f"Artists in common      : {len(overlap)}")
print("Examples:", list(overlap)[:10])

# ── PLOTS ──────────────────────────────────────────────────
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle("EDA - Spotify & Grammys", fontsize=16)

# 1. Popularity distribution
axes[0,0].hist(spotify['popularity'], bins=30, color='steelblue', edgecolor='white')
axes[0,0].set_title("Spotify - Popularity Distribution")
axes[0,0].set_xlabel("Popularity")

# 2. Top 10 genres
top_genres = spotify['track_genre'].value_counts().head(10)
axes[0,1].barh(top_genres.index[::-1], top_genres.values[::-1], color='steelblue')
axes[0,1].set_title("Spotify - Top 10 Genres")

# 3. Grammy awards per year
yearly = grammys['year'].value_counts().sort_index()
axes[1,0].plot(yearly.index, yearly.values, color='goldenrod', marker='o', markersize=3)
axes[1,0].set_title("Grammys - Awards per Year")
axes[1,0].set_xlabel("Year")

# 4. Top 10 Grammy categories
top_cats = grammys['category'].value_counts().head(10)
axes[1,1].barh(top_cats.index[::-1], top_cats.values[::-1], color='goldenrod')
axes[1,1].set_title("Grammys - Top 10 Categories")

plt.tight_layout()
plt.savefig("data/eda_plots.png", dpi=120)
plt.show()
print("\n✅ EDA complete. Plot saved to data/eda_plots.png")
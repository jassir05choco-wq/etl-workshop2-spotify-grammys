from sqlalchemy import create_engine, text

# Connection to MySQL
engine = create_engine(
    "mysql+pymysql://etl_user:etl1234@localhost:3307/grammys_db"
)

sql_statements = [

# ── DIMENSION TABLES ───────────────────────────────────────

"""
CREATE TABLE IF NOT EXISTS dim_artist (
    artist_id     INT AUTO_INCREMENT PRIMARY KEY,
    artist_name   VARCHAR(500) NOT NULL,
    UNIQUE KEY uq_artist (artist_name(255))
)
""",

"""
CREATE TABLE IF NOT EXISTS dim_song (
    song_id          INT AUTO_INCREMENT PRIMARY KEY,
    track_id         VARCHAR(100),
    song_name        VARCHAR(500),
    album            VARCHAR(500),
    track_genre      VARCHAR(100),
    duration_sec     FLOAT,
    explicit         BOOLEAN,
    UNIQUE KEY uq_track (track_id)
)
""",

"""
CREATE TABLE IF NOT EXISTS dim_grammy_category (
    category_id      INT AUTO_INCREMENT PRIMARY KEY,
    category_name    VARCHAR(300) NOT NULL,
    UNIQUE KEY uq_category (category_name(255))
)
""",

"""
CREATE TABLE IF NOT EXISTS dim_date (
    date_id     INT PRIMARY KEY,
    year        INT NOT NULL
)
""",

# ── FACT TABLE ─────────────────────────────────────────────

"""
CREATE TABLE IF NOT EXISTS fact_grammy_spotify (
    fact_id          INT AUTO_INCREMENT PRIMARY KEY,
    artist_id        INT,
    song_id          INT,
    category_id      INT,
    date_id          INT,
    grammy_nominee   VARCHAR(500),
    grammy_winner    BOOLEAN,
    found_in_spotify BOOLEAN,
    popularity       FLOAT,
    danceability     FLOAT,
    energy           FLOAT,
    loudness         FLOAT,
    tempo            FLOAT,
    valence          FLOAT,
    speechiness      FLOAT,
    acousticness     FLOAT,
    instrumentalness FLOAT,
    liveness         FLOAT,
    FOREIGN KEY (artist_id)   REFERENCES dim_artist(artist_id),
    FOREIGN KEY (song_id)     REFERENCES dim_song(song_id),
    FOREIGN KEY (category_id) REFERENCES dim_grammy_category(category_id),
    FOREIGN KEY (date_id)     REFERENCES dim_date(date_id)
)
"""
]

with engine.connect() as conn:
    for stmt in sql_statements:
        conn.execute(text(stmt))
        conn.commit()

print("✅ Data Warehouse schema created successfully.")
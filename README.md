# ETL Workshop 2 — Spotify & Grammys Pipeline

## 📌 Overview
End-to-end ETL pipeline built with Apache Airflow that extracts, transforms, and loads data from two different sources (Spotify CSV and Grammys database), merges them, and loads the result into a Data Warehouse for visualization in Power BI.

---

## 🛠️ Technologies
- **Python 3.11**
- **Apache Airflow 2.10.4**
- **MySQL 8.0** (source database + Data Warehouse)
- **PostgreSQL 15** (Airflow metadata)
- **Docker & Docker Compose**
- **Power BI Desktop**
- **Google Drive API**

---

## 📁 Project Structure
```
etl_workshop2/
├── dags/
│   └── etl_dag.py          # Airflow DAG definition
├── scripts/
│   ├── eda.py              # Exploratory Data Analysis
│   ├── transform.py        # Extraction, cleaning and merge logic
│   ├── create_dw.py        # Data Warehouse schema creation
│   ├── load_dw.py          # Load data into DW
│   ├── load_grammys_to_mysql.py  # Load Grammys CSV into MySQL
│   └── upload_gdrive.py    # Upload merged CSV to Google Drive
├── data/                   # CSV datasets (not tracked by Git)
├── credentials/            # Google credentials (not tracked by Git)
├── plugins/                # Airflow plugins (empty)
├── logs/                   # Airflow logs (not tracked by Git)
├── docker-compose.yml      # Docker services definition
└── README.md
```

---

## 🔄 ETL Pipeline

### Architecture
```
CSV (Spotify) ──────────────────────────────┐
                                            ▼
CSV (Grammys) → MySQL (source) → Airflow → Merge → MySQL (DW) → Power BI
```

### Pipeline Steps
1. **Extract Spotify** — reads `spotify_dataset.csv` directly into Airflow
2. **Extract Grammys** — queries `grammy_awards` table from MySQL
3. **Clean Spotify** — removes duplicates, nulls, renames columns, normalizes artist names
4. **Clean Grammys** — removes technical awards (null artist), normalizes artist names
5. **Merge** — LEFT JOIN on normalized artist name (Grammys → Spotify)
6. **Store CSV** — saves merged dataset as CSV (uploaded to Google Drive)
7. **Load DW** — loads merged data into star schema Data Warehouse

### Airflow DAG
```
extract_spotify ──► clean_spotify ──┐
                                    ├──► merge ──► store_gdrive
extract_grammys ──► clean_grammys ──┘         └──► load_dw
```

---

## 🗄️ Data Model (Star Schema)

### Grain
One row per Grammy winner appearance, optionally enriched with Spotify track data.

### Dimensions
| Table | Description |
|---|---|
| `dim_artist` | Grammy winner artists |
| `dim_song` | Spotify track details |
| `dim_grammy_category` | Grammy award categories |
| `dim_date` | Year dimension (1958-2019) |

### Fact Table
| Table | Measures |
|---|---|
| `fact_grammy_spotify` | popularity, danceability, energy, tempo, valence, found_in_spotify |

---

## 📊 KPIs & Visualizations (Power BI)

### KPIs
1. **Total Grammy Winner Artists** — count of unique artists
2. **Average Spotify Popularity** — mean popularity score of Grammy artists
3. **Songs Found in Spotify** — total Grammy songs matched in Spotify

### Charts
1. **Top 10 Artists by Avg Spotify Popularity** — bar chart (Beyoncé leads with ~94)
2. **Grammy Awards per Year** — line chart (1958-2019 trend)
3. **Avg Popularity by Genre** — horizontal bar chart (death-metal and ambient lead)

---

## 🚀 Setup Instructions

### Prerequisites
- Docker Desktop
- Python 3.11+
- Power BI Desktop
- MySQL ODBC Driver 9.6

### 1 — Clone the repository
```bash
git clone https://github.com/jassir05choco-wq/etl-workshop2-spotify-grammys.git
cd etl-workshop2-spotify-grammys
```

### 2 — Add datasets
Place these files in the `data/` folder:
- `spotify_dataset.csv`
- `the_grammy_awards.csv`

### 3 — Start Docker services
```bash
docker-compose up airflow-init
docker-compose up -d
```

### 4 — Load Grammys into MySQL
```bash
python scripts/load_grammys_to_mysql.py
```

### 5 — Create Data Warehouse schema
```bash
python scripts/create_dw.py
```

### 6 — Access Airflow UI
Open http://localhost:8081 — user: `admin` / password: `admin`
Trigger the `etl_spotify_grammys` DAG manually.

### 7 — Connect Power BI
Use ODBC connector with:
```
Driver={MySQL ODBC 9.6 Unicode Driver};Server=localhost;Port=3307;Database=grammys_db;
```

---

## 🔍 Key Decisions & Assumptions

- **Merge strategy**: LEFT JOIN from Grammys to Spotify on normalized artist name. This keeps all Grammy winners even if not found in Spotify (88% match rate — 14,558 of 16,517 rows).
- **Duplicate handling**: Spotify has 24,259 duplicate track_ids (same song in multiple genres). We kept the entry with highest popularity.
- **Grammys nulls**: 1,840 rows with null artist were dropped — these are technical/production awards without a main artist.
- **Star schema**: Chosen for its simplicity and compatibility with BI tools. Supports all required analytical queries.
- **Google Drive**: CSV uploaded manually due to service account storage quota limitations on personal Google accounts.

---

## 👤 Author
**Jassir** — Data Engineering & AI Student  
Universidad Autónoma de Occidente

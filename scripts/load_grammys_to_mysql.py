import pandas as pd
from sqlalchemy import create_engine

# Connection to MySQL running in Docker
engine = create_engine(
    "mysql+pymysql://etl_user:etl1234@localhost:3307/grammys_db"
)

# Load CSV
df = pd.read_csv("data/the_grammy_awards.csv")

print("Shape:", df.shape)
print("Columns:", df.columns.tolist())
print("Nulls:\n", df.isnull().sum())
print(df.head(3))

# Load into MySQL
df.to_sql(
    name="grammy_awards",
    con=engine,
    if_exists="replace",
    index=False
)

print("✅ Grammys data loaded into MySQL successfully.")
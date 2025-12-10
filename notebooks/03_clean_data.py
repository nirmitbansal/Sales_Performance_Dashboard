import pandas as pd
from sqlalchemy import create_engine
import urllib.parse

# ---------- MySQL Config (replace only password) ----------
MYSQL_USER = "nirmitbansal"
MYSQL_PW   = "nirmit@2708"   # replace this
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_DB   = "retail_sales"

pw_esc = urllib.parse.quote_plus(MYSQL_PW)

engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{pw_esc}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
)

# ---------- Read data from MySQL ----------
df = pd.read_sql("SELECT * FROM sales", engine)

print("Loaded rows:", len(df))

# ---------- Convert dates properly ----------
df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
df['ship_date']  = pd.to_datetime(df['ship_date'], errors='coerce')

# Verify conversion
print("order_date type:", df['order_date'].dtype)

# ---------- Feature Engineering ----------
df['year']  = df['order_date'].dt.year
df['month'] = df['order_date'].dt.month
df['profit_margin'] = df['profit'] / df['sales']

# ---------- Export Clean Data ----------
output_path = r"D:\Nirmit\Retail-Sales-Analysis\data\clean_sales.csv"
df.to_csv(output_path, index=False)

print("Clean dataset exported to:", output_path)
print("Final rows:", len(df))

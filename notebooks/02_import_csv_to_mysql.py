# 02_import_csv_to_mysql.py
# Full working script with correct indentation and safe password handling

import pandas as pd
from sqlalchemy import create_engine, types
import urllib.parse

# -------------------------------------------------------
# MySQL CONFIG — REPLACE ONLY THE PASSWORD
# -------------------------------------------------------
MYSQL_USER = "nirmitbansal"
MYSQL_PW   = "nirmit@2708"   # ← REPLACE THIS
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_DB   = "retail_sales"

CSV_PATH   = r"D:\Nirmit\Retail-Sales-Analysis\data\superstore.csv"
# -------------------------------------------------------


def main():

    # Read CSV
    try:
        df = pd.read_csv(CSV_PATH, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(CSV_PATH, encoding="latin1")

    print("CSV columns:", df.columns.tolist())
    print("Preview rows:", df.shape[0])

    # Rename cols to match MySQL
    df = df.rename(columns={
        'Row ID': 'row_id',
        'Order ID': 'order_id',
        'Order Date': 'order_date',
        'Ship Date': 'ship_date',
        'Ship Mode': 'ship_mode',
        'Customer ID': 'customer_id',
        'Customer Name': 'customer_name',
        'Segment': 'segment',
        'Country': 'country',
        'City': 'city',
        'State': 'state_name',
        'Postal Code': 'postal_code',
        'Region': 'region',
        'Product ID': 'product_id',
        'Category': 'category',
        'Sub-Category': 'sub_category',
        'Product Name': 'product_name',
        'Sales': 'sales',
        'Quantity': 'quantity',
        'Discount': 'discount',
        'Profit': 'profit'
    })

    # Convert dates
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce', format='%m/%d/%Y')
    df['ship_date']  = pd.to_datetime(df['ship_date'], errors='coerce', format='%m/%d/%Y')

    # Clean numeric
    df['sales']    = pd.to_numeric(df['sales'], errors='coerce')
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['discount'] = pd.to_numeric(df['discount'], errors='coerce')
    df['profit']   = pd.to_numeric(df['profit'], errors='coerce')

    df = df.dropna(subset=['order_id', 'order_date'])

    # -------------------------------------------------------
    # Create engine with SAFE password encoding
    # -------------------------------------------------------
    pw_esc = urllib.parse.quote_plus(MYSQL_PW)

    engine = create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{pw_esc}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}",
        connect_args={"connect_timeout": 10}
    )
    # -------------------------------------------------------

    # SQL column types
    dtype_map = {
        'row_id': types.INTEGER(),
        'order_id': types.VARCHAR(50),
        'order_date': types.DATE(),
        'ship_date': types.DATE(),
        'ship_mode': types.VARCHAR(50),
        'customer_id': types.VARCHAR(50),
        'customer_name': types.VARCHAR(100),
        'segment': types.VARCHAR(50),
        'country': types.VARCHAR(50),
        'city': types.VARCHAR(50),
        'state_name': types.VARCHAR(100),
        'postal_code': types.VARCHAR(20),
        'region': types.VARCHAR(50),
        'product_id': types.VARCHAR(50),
        'category': types.VARCHAR(50),
        'sub_category': types.VARCHAR(50),
        'product_name': types.VARCHAR(200),
        'sales': types.DECIMAL(12, 2),
        'quantity': types.INTEGER(),
        'discount': types.DECIMAL(6, 2),
        'profit': types.DECIMAL(12, 2)
    }

    # Import to MySQL
    try:
        df.to_sql(
            'sales', engine, if_exists='append', index=False,
            dtype=dtype_map, chunksize=1000, method='multi'
        )
        print("Import completed. Rows imported:", len(df))
    except Exception as e:
        print("Import failed:", e)


if __name__ == "__main__":
    main()

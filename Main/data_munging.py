import sqlite3
import pandas as pd

# Step 1: Load the CSV files
data_0 = pd.read_csv('../data/shipping_data_0.csv')
data_1 = pd.read_csv('../data/shipping_data_1.csv')
data_2 = pd.read_csv('../data/shipping_data_2.csv')


# Step 2: Merge data_1 and data_2 based on shipment_identifier
data_merged = pd.merge(data_1, data_2, on='shipment_identifier')

# Step 3: Set up SQLite database
conn = sqlite3.connect("walmart_shipping.db")
cursor = conn.cursor()

# Step 4: Create tables
cursor.execute("""
CREATE TABLE IF NOT EXISTS shipping_data (
    origin_warehouse TEXT,
    destination_store TEXT,
    product TEXT,
    on_time TEXT,
    product_quantity INTEGER,
    driver_identifier TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS shipment_details (
    shipment_identifier TEXT,
    product TEXT,
    on_time TEXT,
    origin_warehouse TEXT,
    destination_store TEXT,
    driver_identifier TEXT
)
""")

# Step 5: Insert data from data_0 directly into the shipping_data table
data_0.to_sql("shipping_data", conn, if_exists="append", index=False)

# Step 6: Insert merged data into the shipment_details table
data_merged.to_sql("shipment_details", conn, if_exists="append", index=False)

# Commit and close the connection
conn.commit()
conn.close()

print("Data successfully inserted into the SQLite database.")

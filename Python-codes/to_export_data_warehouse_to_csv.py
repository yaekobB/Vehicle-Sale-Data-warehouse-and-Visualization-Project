import pandas as pd
import psycopg2

# === 1. Database connection settings ===
conn = psycopg2.connect(
    host="localhost",        # or your DB host
    port="5432",             # default PostgreSQL port
    dbname="vehicle_sales_db",  # replace with your DB name
    user="postgres",    # replace with your DB user
    password="pgAdmin4" # replace with your DB password
)

# === 2. Your large query ===
query = """
SELECT 
    f.sale_id, f.sellingprice, f.mmr, f.odometer, f.condition,
    t.year, t.quarter, t.month, t.day,
    v.vin, v.make, v.model, v.make_category, v.trim, v.body, v.year as model_year, v.transmission, v.color, v.interior,
    l.state, l.state_full_name, l.region,
    s.seller_name,
    c.condition_category
FROM dw.fact_sales f
JOIN dw.dim_time t ON f.time_id = t.time_id
JOIN dw.dim_vehicle v ON f.vehicle_id = v.vehicle_id
JOIN dw.dim_location l ON f.location_id = l.location_id
JOIN dw.dim_seller s ON f.seller_id = s.seller_id
JOIN dw.dim_condition c ON f.condition_id = c.condition_id;

"""

# === 3. Export in chunks ===
output_file = "vehicle_sales_dw_export.csv"
chunk_size = 50000  # rows per chunk

with conn.cursor(name='cursor_for_large_export') as cur:
    cur.itersize = chunk_size
    cur.execute(query)

    # Write the first chunk with header
    first_chunk = True
    while True:
        rows = cur.fetchmany(chunk_size)
        if not rows:
            break
        df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
        df.to_csv(output_file, mode='a', header=first_chunk, index=False)
        first_chunk = False
        print(f"{len(df)} rows written...")

print(f"✅ Done. Full export saved to {output_file}")
conn.close()

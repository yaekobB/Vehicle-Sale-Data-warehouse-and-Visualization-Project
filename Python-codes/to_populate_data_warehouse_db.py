import pandas as pd
from sqlalchemy import create_engine

# Database connection (update credentials)
#engine = create_engine('postgresql+psycopg2://user:password@localhost:5432/vehicle_sales_db')
engine = create_engine('postgresql+psycopg2://postgres:pgAdmin4@localhost:5432/vehicle_sales_db')


# Load from reconciled.sale
df = pd.read_sql("""
    SELECT s.vin, s.saledate, s.condition, s.condition_category, s.odometer, s.mmr, s.sellingprice,
           v.make, v.make_category, v.model, v.trim, v.body, v.year, v.transmission, v.color, v.interior,
           l.state, l.state_full_name, l.region, se.seller_name as seller
    FROM reconciled.sale s
    JOIN reconciled.vehicle v ON s.vin = v.vin
    JOIN reconciled.seller se ON s.seller_id = se.seller_id
    JOIN reconciled.location l ON s.location_id = l.location_id
""", engine)
print(f"Loaded data with {len(df)} rows")  # Expect 465248

# Set schema
schema = 'dw'

# Populate dim_time
df['saledate'] = pd.to_datetime(df['saledate'], errors='coerce', utc=True)
dates = df['saledate'].dropna().dt.date.unique()
time_df = pd.DataFrame({
    'year': [d.year for d in dates],
    'quarter': [(d.month-1)//3 + 1 for d in dates],
    'month': [d.month for d in dates],
    'day': [d.day for d in dates]
}).drop_duplicates()
time_df.to_sql('dim_time', engine, schema=schema, if_exists='append', index=False)
print(f"Populated dim_time with {len(time_df)} rows")

# Populate dim_location
location_df = df[['state', 'state_full_name', 'region']].drop_duplicates()
location_df['state'] = location_df['state'].str.lower()
location_df.to_sql('dim_location', engine, schema=schema, if_exists='append', index=False)
print(f"Populated dim_location with {len(location_df)} rows")

# Populate dim_vehicle
vehicle_df = df[['vin', 'make', 'model', 'trim', 'body', 'year', 'transmission', 'color', 'interior', 'make_category']].drop_duplicates('vin')
vehicle_df.to_sql('dim_vehicle', engine, schema=schema, if_exists='append', index=False)
print(f"Populated dim_vehicle with {len(vehicle_df)} rows")

# Populate dim_seller
seller_df = df[['seller']].drop_duplicates().reset_index(drop=True)
seller_df.columns = ['seller_name']
seller_df['seller_name'] = seller_df['seller_name'].str.lower()
seller_df.to_sql('dim_seller', engine, schema=schema, if_exists='append', index=False)
print(f"Populated dim_seller with {len(seller_df)} rows")

# Populate dim_condition
condition_df = df[['condition_category']].drop_duplicates().reset_index(drop=True)
condition_df.to_sql('dim_condition', engine, schema=schema, if_exists='append', index=False)
print(f"Populated dim_condition with {len(condition_df)} rows")

# Populate fact_sales
time_map = pd.read_sql("SELECT time_id, year, month, day FROM dw.dim_time", engine)
time_map['date'] = pd.to_datetime(time_map[['year', 'month', 'day']], errors='coerce')
seller_map = pd.read_sql("SELECT seller_id, seller_name FROM dw.dim_seller", engine)
location_map = pd.read_sql("SELECT location_id, state FROM dw.dim_location", engine)
vehicle_map = pd.read_sql("SELECT vehicle_id, vin FROM dw.dim_vehicle", engine)
condition_map = pd.read_sql("SELECT condition_id, condition_category FROM dw.dim_condition", engine)

# Replace fact_df section in populate_dw.py
fact_df = df[['vin', 'saledate', 'state', 'seller', 'condition_category', 'sellingprice', 'mmr', 'odometer', 'condition']].copy()
fact_df.loc[:, 'state'] = fact_df['state'].str.lower()
fact_df.loc[:, 'seller'] = fact_df['seller'].str.lower()
fact_df['saledate'] = pd.to_datetime(fact_df['saledate'], errors='coerce', utc=True)
fact_df = fact_df.merge(vehicle_map, on='vin') \
                 .merge(location_map, on='state') \
                 .merge(seller_map, left_on='seller', right_on='seller_name') \
                 .merge(condition_map, on='condition_category')
                 
fact_df = fact_df.merge(time_map, left_on=fact_df['saledate'].dt.date.astype('datetime64[ns]'), right_on='date')

fact_df = fact_df[['vehicle_id', 'time_id', 'location_id', 'seller_id', 'condition_id', 'sellingprice', 'mmr', 'odometer', 'condition']]
fact_df['sale_id'] = range(1, len(fact_df) + 1)
fact_df = fact_df[['sale_id', 'time_id', 'vehicle_id', 'location_id', 'seller_id', 'condition_id', 'sellingprice', 'mmr', 'odometer', 'condition']]

print("About to insert fact_sales rows:", len(fact_df))
print(fact_df.head())

fact_df.to_sql('fact_sales', engine, schema=schema, if_exists='append', index=False)
print(f"Populated fact_sales with {len(fact_df)} rows")
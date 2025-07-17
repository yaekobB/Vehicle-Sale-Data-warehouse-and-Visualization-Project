import pandas as pd
from sqlalchemy import create_engine

# Database connection (update with your PostgreSQL credentials)
engine = create_engine('postgresql+psycopg2://postgres:pgAdmin4@localhost:5432/vehicle_sales_db')

# Load cleaned CSV
df = pd.read_csv('vehicleSale_unique_final_2000-2015.csv')
print(f"Loaded CSV with {len(df)} rows")  # Should be 465248

# Set schema
schema = 'reconciled'

# Populate Seller
seller_df = df[['seller']].drop_duplicates().reset_index(drop=True)
seller_df.columns = ['seller_name']
seller_df.to_sql('seller', engine, schema=schema, if_exists='append', index=False)
print(f"Populated Seller with {len(seller_df)} rows")

# Populate Location
location_df = df[['state', 'state_full_name', 'region']].drop_duplicates()
location_df.to_sql('location', engine, schema=schema, if_exists='append', index=False)
print(f"Populated Location with {len(location_df)} rows")

# Populate Vehicle
vehicle_df = df[['vin', 'make', 'model', 'trim', 'body', 'year', 'transmission', 'color', 'interior', 'make_category']].drop_duplicates('vin')
vehicle_df.to_sql('vehicle', engine, schema=schema, if_exists='append', index=False)
print(f"Populated Vehicle with {len(vehicle_df)} rows")

# Populate Sale
seller_map = pd.read_sql("SELECT seller_id, seller_name FROM reconciled.seller", engine)
location_map = pd.read_sql("SELECT location_id, state FROM reconciled.location", engine)

print("\nSample seller values:", df['seller'].dropna().unique()[:5])
print("Sample state values:", df['state'].dropna().unique()[:5])
print("Seller map sample:", seller_map.head())
print("Location map sample:", location_map.head())


sale_df = df[['vin', 'seller', 'state', 'saledate', 'condition', 'condition_category', 'odometer', 'mmr', 'sellingprice']]
sale_df = sale_df.merge(seller_map, left_on='seller', right_on='seller_name').merge(location_map, on='state')
sale_df = sale_df[['vin', 'seller_id', 'location_id', 'saledate', 'condition', 'condition_category', 'odometer', 'mmr', 'sellingprice']]
sale_df['saledate'] = pd.to_datetime(sale_df['saledate'], errors='coerce')
sale_df.to_sql('sale', engine, schema=schema, if_exists='append', index=False)
print(f"\nPopulated Sale with {len(sale_df)} rows")
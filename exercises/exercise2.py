import pandas as pd
import os
import requests
import io
import re
from sqlalchemy import create_engine , types
url = 'https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/stadt-neuss-herbstpflanzung-2023/exports/csv'
response = requests.get(url)
# Function to validate geopoint IDs
def is_valid_geopoint(geopoint):
    pattern = r'^(\d{1,3}\.\d+), (\d{1,3}\.\d+)$'
    return bool(re.match(pattern, geopoint))
if response.status_code == 200:
    # Use StringIO to handle the CSV data as a string

    data = io.StringIO(response.content.decode('utf-8'))
    df = pd.read_csv(data , delimiter = ';')
    df = df[df['stadtteil'].str.startswith("Furth-")]
    df = df[df['id'].apply(is_valid_geopoint)]
    if 'baumart_deutsch' in df.columns:
        df.drop('baumart_deutsch', axis=1, inplace=True)
    # print(df.isnull().sum())    
    sqlite_types = {
        # "baumart_botanisch": types.TEXT(),

        "id": types.TEXT(),
        "baumfamilie": types.TEXT(),
        "standort": types.TEXT(),
        "stadtteil": types.TEXT(),
        "lfd_nr": types.INT()
    }
    df.dropna(inplace=True)
    # print(df.columns)    
 
    db_file_path = os.path.join("./exercises/trees.sqlite")
    engine = create_engine(f"sqlite:///{db_file_path}")# Write the DataFrame to the SQLite database

    df.to_sql("trees", engine, index=False, if_exists="replace", dtype=sqlite_types)
  



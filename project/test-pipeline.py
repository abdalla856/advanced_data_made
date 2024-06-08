import os
import sqlite3
import pandas as pd
import pytest
import ssl

ssl._create_default_https_context = ssl._create_stdlib_context

def test_load_data():
    url_temp = "https://opendata.arcgis.com/datasets/4063314923d74187be9596f10d034914_0.csv"
    url_sea = "https://opendata.arcgis.com/datasets/b84a7e25159b4c65ba62d3f82c605855_0.csv"

    temperature_data = pd.read_csv(url_temp)
    sea_level_data_original = pd.read_csv(url_sea)

    assert not temperature_data.empty, "Temperature data is not loaded correctly"
    assert not sea_level_data_original.empty, "Sea level data is not loaded correctly"

def test_data_cleaning():
    url_sea = "https://opendata.arcgis.com/datasets/b84a7e25159b4c65ba62d3f82c605855_0.csv"
    sea_level_data_original = pd.read_csv(url_sea)

    sea_level_data_original['Date'] = sea_level_data_original['Date'].astype(str)
    sea_level_data_original['Date'] = sea_level_data_original['Date'].str[1:]
    sea_level_data_original['Date'] = pd.to_datetime(sea_level_data_original['Date'], format='%m/%d/%Y')
    sea_level_data_original['Year'] = sea_level_data_original['Date'].dt.year

    assert 'Date' in sea_level_data_original.columns, "Date column is missing"
    assert sea_level_data_original['Date'].dtype == 'datetime64[ns]', "Date column is not in datetime format"
    assert 'Year' in sea_level_data_original.columns, "Year column is missing"
    assert sea_level_data_original['Year'].dtype == 'int32', "Year column is not in integer format"

def test_data_merging():
    url_temp = "https://opendata.arcgis.com/datasets/4063314923d74187be9596f10d034914_0.csv"
    url_sea = "https://opendata.arcgis.com/datasets/b84a7e25159b4c65ba62d3f82c605855_0.csv"

    temperature_data = pd.read_csv(url_temp)
    sea_level_data_original = pd.read_csv(url_sea)

    sea_level_data_original['Date'] = sea_level_data_original['Date'].astype(str)
    sea_level_data_original['Date'] = sea_level_data_original['Date'].str[1:]
    sea_level_data_original['Date'] = pd.to_datetime(sea_level_data_original['Date'], format='%m/%d/%Y')
    sea_level_data_original['Year'] = sea_level_data_original['Date'].dt.year

    temperature_data_long = temperature_data.melt(
        id_vars=['Country', 'ISO2', 'ISO3', 'Indicator', 'Unit', 'Source', 'CTS_Code', 'CTS_Name', 'CTS_Full_Descriptor'],
        var_name='Year',
        value_name='Temperature_Change'
    )
    temperature_data_long['Year'] = temperature_data_long['Year'].str.extract(r'(\d+)')
    temperature_data_long = temperature_data_long.dropna(subset=['Year', 'Temperature_Change'])
    temperature_data_long['Year'] = temperature_data_long['Year'].astype(int)

    numeric_columns = sea_level_data_original.select_dtypes(include='number').columns.tolist()
    numeric_columns = [col for col in numeric_columns if col != 'Year']  # Exclude 'Year' column

    sea_level_data_agg = sea_level_data_original.groupby(['Year', 'Measure'])[numeric_columns].mean().reset_index()
    merged_data_cleaned = pd.merge(temperature_data_long, sea_level_data_agg, on='Year', how='inner')

    assert not merged_data_cleaned.empty, "Merged data is empty"
    assert 'Year' in merged_data_cleaned.columns, "Year column is missing in merged data"
    assert 'Temperature_Change' in merged_data_cleaned.columns, "Temperature_Change column is missing in merged data"

def test_database_content():
    db_path = '../data/temperature_sea_level_data.sqlite'
    conn = sqlite3.connect(db_path)

    df = pd.read_sql_query("SELECT * FROM Temperature_Sea_Level", conn)

    expected_columns = ['Year', 'Temperature_Change', 'Measure'] + [col for col in df.columns if col not in ['Year', 'Temperature_Change', 'Measure']]
    for column in expected_columns:
        assert column in df.columns, f"{column} is missing from the database table"

    conn.close()

def test_data_pipeline_output():
    db_path = '../data/temperature_sea_level_data.sqlite'

    # Check if the database file is created
    assert os.path.exists(db_path), "Output database file does not exist"

    # Connect to the database and check if the table exists and has data
    conn = sqlite3.connect(db_path)
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name='Temperature_Sea_Level';"
    result = conn.execute(query).fetchall()
    assert len(result) == 1, "Table 'Temperature_Sea_Level' does not exist in the database"

    # Check if the table has data
    df = pd.read_sql_query("SELECT * FROM Temperature_Sea_Level", conn)
    assert not df.empty, "Table 'Temperature_Sea_Level' is empty"

    conn.close()

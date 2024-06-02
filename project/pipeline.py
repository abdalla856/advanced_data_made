import pandas as pd
import sqlite3
import ssl

ssl._create_default_https_context = ssl._create_stdlib_context
url_temp = "https://opendata.arcgis.com/datasets/4063314923d74187be9596f10d034914_0.csv"
url_sea = "https://opendata.arcgis.com/datasets/b84a7e25159b4c65ba62d3f82c605855_0.csv"

temperature_data = pd.read_csv(url_temp)
sea_level_data_original = pd.read_csv(url_sea)


# Ensure that the 'Date' column is of type string before stripping the leading 'D'
sea_level_data_original['Date'] = sea_level_data_original['Date'].astype(str)

# Remove the leading "D" from the 'Date' column
sea_level_data_original['Date'] = sea_level_data_original['Date'].str[1:]

# Convert the 'Date' column to datetime format
sea_level_data_original['Date'] = pd.to_datetime(sea_level_data_original['Date'], format='%m/%d/%Y')


# Extract the year from the 'Date' column
sea_level_data_original['Year'] = sea_level_data_original['Date'].dt.year

## Drop Uncessary Columns# 
sea_level_data_original.drop(
    columns=[
        "ISO2",
        "ISO3",
        "ObjectId",
        "Indicator",
        "Source",
        "CTS_Code",
        "CTS_Name",
        "CTS_Full_Descriptor",
        'Unit'
    ],
    inplace=True,
)


# Drop rows with any NaN values
print("Nan Values",sea_level_data_original.isna().sum())    

# sea_level_data_original = sea_level_data_original.dropna(subset=['Year'])
# print("sea",sea_level_data_original.head())


# Select only the numeric columns for aggregation, excluding 'Date' and non-numeric columns
numeric_columns = sea_level_data_original.select_dtypes(include='number').columns.tolist()
numeric_columns = [col for col in numeric_columns if col != 'Year']  # Exclude 'Year' column

# Aggregate sea level data by year to prepare for merging
sea_level_data_agg = sea_level_data_original.groupby(['Year' , "ق"])[numeric_columns].mean().reset_index()
# Restructure temperature data to focus on years and temperature change values
temperature_data_long = temperature_data.melt(
    id_vars=['Country', 'ISO2', 'ISO3', 'Indicator', 'Unit', 'Source', 'CTS_Code', 'CTS_Name', 'CTS_Full_Descriptor'],
    var_name='Year',
    value_name='Temperature_Change'
)
temperature_data_long['Year'] = temperature_data_long['Year'].str.extract(r'(\d+)')

# Drop rows with any NaN values in temperature data
temperature_data_long = temperature_data_long.dropna(subset=['Year', 'Temperature_Change'])

# Ensure 'Year' is integer
temperature_data_long['Year'] = temperature_data_long['Year'].astype(int)
print(temperature_data_long.head())

# Aggregate sea level data by year to prepare for merging
# sea_level_data_agg = sea_level_data_original.groupby('Year').mean().reset_index()
print(sea_level_data_original.head())


# Merge datasets on 'Year' column
merged_data_cleaned = pd.merge(temperature_data_long, sea_level_data_agg, on='Year', how='inner')

# Rename the 'Value' column to 'Mean_Sea_Level_Change'
merged_data_cleaned.rename(columns={'Value': 'Mean_Sea_Level_Change'}, inplace=True)

# Drop rows with any NaN values in the merged data
merged_data_cleaned = merged_data_cleaned.dropna()




## Drop merged Data


merged_data_cleaned.drop(
    columns=[
        "ISO2",
        "ISO3",
        # "ObjectId",
        "Indicator",
        "Source",
        "CTS_Code",
        "CTS_Name",
        "CTS_Full_Descriptor",
        'Unit'
    ],
    inplace=True,
)  

print("merged data " ,merged_data_cleaned.head())



conn = sqlite3.connect('../data/temperature_sea_level_data.db')
merged_data_cleaned.to_sql('Temperature_Sea_Level', conn, if_exists='replace', index=False)
conn.close()
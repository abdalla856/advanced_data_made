import pandas as pd
import sqlite3
# URL of the CSV file
### Read the datasets from the internt .
url_temp = "https://opendata.arcgis.com/datasets/4063314923d74187be9596f10d034914_0.csv"
url_sea = "https://opendata.arcgis.com/datasets/b84a7e25159b4c65ba62d3f82c605855_0.csv"
# Read the CSV file from the URL


data_temp = pd.read_csv(url_temp)
data_sea = pd.read_csv(url_sea)
### drop the rows that has a Nan values

data_temp.dropna(inplace=True)


### drop drop unrealated Columns

data_temp.drop(
    columns=[
        "ISO2",
        "ISO3",
        "ObjectId",
        "Indicator",
        "Unit",
        "Source",
        "CTS_Name",
        "CTS_Code",
        "CTS_Full_Descriptor",
    ],
    inplace=True,
)
data_sea.drop(
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

# Display the number of null values in the datasets
print(data_temp.isna().sum())
print(data_sea.isna().sum())

# Create or connect to a SQLite database
conn = sqlite3.connect('../data/sea_climate.sqlite')

# Write the data to a new table called 'sea_climate'
data_sea.to_sql('sea_climate', conn, if_exists='replace', index=False)

# Close the connection
conn.close()




# Create or connect to a SQLite database
conn = sqlite3.connect('../data/temp_climate.sqlite')

# Write the data to a new table called 'temp_climate'
data_temp.to_sql('temp_climate', conn, if_exists='replace', index=False)

# Close the connection
conn.close()
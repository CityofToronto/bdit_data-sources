import os
import chardet
import pandas as pd
from sqlalchemy import create_engine, Integer, String
from pathlib import Path
from os.path import expanduser

# Function to create the database engine using credentials stored in .pgpass
def create_db_pgpass_engine():
    with open(expanduser('~/.pgpass'), 'r') as f:
        host, port, database, user, password = f.read().split(':')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    return engine

# Function to process and load the profile data to the database
def export_and_load_profile_data():
    # Used "98-401-X2021006_Geo_starting_row_Ontario file" to find the start and end lines.
    # It only specifies the start line, and we need to calculate the end line number.
    # The dissemination area id for Toronto starts at 35200002 and ends at 35205069 and each should have 2631 census items.
    input_file = './98-401-X2021006_English_TAB_data_Ontario.TAB'

    # Calculate the range of lines to read
    start_line = 14409988
    end_line = 24257820
    total_lines_to_read = end_line - start_line + 1

    # Detect file encoding by reading the first ~1MB
    # I am reading the census file including everything. It's around 7Gb when unzipped. Therefore, I read each 1MB by 1MB
    with open(input_file, 'rb') as f:
        result = chardet.detect(f.read(1_000_000))
    encoding = result['encoding']
    print(f"Detected encoding: {encoding}")

    # Check Column Name
    df_header = pd.read_csv(
        input_file,
        sep='\t',
        encoding=encoding,
        dtype=str,
        nrows=3)
    print("Available columns:", df_header.columns.tolist())

    # Initialize the database engine
    db_engine = create_db_pgpass_engine()

    # Process the file in chunks
    chunksize = 10**6
    df_chunks = pd.read_csv(
        input_file,
        sep='\t',
        encoding=encoding,
        chunksize=chunksize,
        skiprows=range(1, start_line),  # Skip lines before start_line
        nrows=total_lines_to_read,
        dtype=str
    )

    # Columns we need to extract
    census_profile_columns = ['ALT_GEO_CODE', 'CHARACTERISTIC_ID', 'C1_COUNT_TOTAL']

    for i, chunk in enumerate(df_chunks):
        if set(census_profile_columns).issubset(chunk.columns):
            # Select and rename the required columns
            census_profile = chunk[census_profile_columns].rename(
                columns={
                    'ALT_GEO_CODE': 'alt_geo_code',
                    'CHARACTERISTIC_ID': 'var_id',
                    'C1_COUNT_TOTAL': 'val'
                }
            )

            # Ensure alt_geo_code is an integer
            census_profile['alt_geo_code'] = pd.to_numeric(census_profile['alt_geo_code'], errors='coerce', downcast='integer')
            census_profile['var_id'] = pd.to_numeric(census_profile['var_id'], errors='coerce', downcast='integer')

            # Remove rows where 'alt_geo_code' or 'var_id' could not be converted to valid integers
            census_profile = census_profile.dropna(subset=['alt_geo_code', 'var_id'])

            # Insert the data directly into the database
            census_profile.to_sql(
                name='profile_data',
                con=db_engine,
                schema='census2021',
                if_exists='append',  # Append data to the table
                index=False,
                dtype={
                    'alt_geo_code': Integer(),
                    'var_id': Integer(),
                    'val': String(),
                }
            )
            print(f"Processed and inserted chunk {i} for profile_data into the database.")
        else:
            print(f"Chunk {i} does not contain required columns.")

# Function to export and load the data dictionary to the database
def export_and_load_profile_datadict():
    input_file = './98-401-X2021006_English_TAB_data_Ontario.TAB'

    # Detect encoding
    with open(input_file, 'rb') as f:
        result = chardet.detect(f.read(1_000_000))
    encoding = result['encoding']
    print(f"Detected encoding for dictionary: {encoding}")

    # Read the first 3000 (Census items for 2021 are 2631 items) rows for the dictionary information.
    df = pd.read_csv(input_file, sep='\t', encoding=encoding, dtype=str, nrows=3000)

    dict_columns = ['CHARACTERISTIC_ID', 'CHARACTERISTIC_NAME']
    if set(dict_columns).issubset(df.columns):
        profile_dict = (
            df[dict_columns]
            .rename(columns={
                'CHARACTERISTIC_ID': 'id',
                'CHARACTERISTIC_NAME': 'description'
            })
            .drop_duplicates()
        )

        # Ensure 'id' is an integer
        profile_dict['id'] = pd.to_numeric(profile_dict['id'], errors='coerce', downcast='integer')

        # Insert data dictionary directly into the database
        db_engine = create_db_pgpass_engine()
        profile_dict.to_sql(
            name='profile_datadict',
            con=db_engine,
            schema='census2021',
            if_exists='replace',  # Replace the table if it exists
            index=False,
            dtype={
                'id': Integer(),  # Ensure it's set as Integer
                'description': String()
            }
        )
        print("Exported and loaded profile_datadict to the database.")
    else:
        print("Dictionary columns not found in the file.")

def main():
    export_and_load_profile_data()
    export_and_load_profile_datadict()

if __name__ == "__main__":
    main()
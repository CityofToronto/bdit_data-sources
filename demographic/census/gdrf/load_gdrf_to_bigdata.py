import pandas as pd
from pathlib import Path
from os.path import expanduser
from sqlalchemy import create_engine
from sqlalchemy import text

# Function to create the database engine
def create_db_pgpass_engine():
    with open(expanduser('~/.pgpass'), 'r') as f:
        host, port, database, user, password = f.read().strip().split(':')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    return engine

engine = create_db_pgpass_engine()

# Define DGRF columns (lowercase to match database table)
columns = [
    "prdguid_pridugd",
    "cddguid_dridugd",
    "feddguid_cefidugd",
    "csddguid_sdridugd",
    "erdguid_reidugd",
    "cardguid_raridugd",
    "ccsdguid_sruidugd",
    "dadguid_adidugd",
    "dbdguid_ididugd",
    "adadguid_adaidugd",
    "dpldguid_ldidugd",
    "cmapdguid_rmrpidugd",
    "cmadguid_rmridugd",
    "ctdguid_sridugd",
    "popctrpdguid_ctrpoppidugd",
    "popctrdguid_ctrpopidugd"
]

# Read the DGRF CSV file
try:
    dgrf = pd.read_csv("2021_98260004.csv", dtype=str, names=columns, header=0)
except FileNotFoundError:
    print("Error: CSV file '2021_98260004.csv' not found.")
    exit(1)

# Verify CSV data
print("First 5 rows of raw CSV data:")
print(dgrf.head(5))
print("\nColumn names in DataFrame:")
print(dgrf.columns.tolist())

# Check for Ontario records
print("\nNumber of Ontario records (before filtering):")
ontario_count = len(dgrf[dgrf["prdguid_pridugd"] == "2021A000235"])
print(ontario_count)
if ontario_count == 0:
    print("Warning: No Ontario records found in CSV. Check if the correct file is used.")

# Filter for Ontario
ontario_dgrf = dgrf[dgrf["prdguid_pridugd"] == "2021A000235"]

# Filter for GTHA Census Divisions
gtha_cd_dguids = [
    "2021A00033518",  # Durham
    "2021A00033519",  # York
    "2021A00033520",  # Toronto
    "2021A00033521",  # Peel
    "2021A00033524",  # Halton
    "2021A00033525"   # Hamilton
]
gtha_dgrf = ontario_dgrf[ontario_dgrf["cddguid_dridugd"].isin(gtha_cd_dguids)]

# Verify filtered data
print("\nNumber of GTHA records after filtering:")
print(len(gtha_dgrf))
if len(gtha_dgrf) == 0:
    print("Warning: No GTHA records found. Check CSV data or filter criteria.")

# Check for NaN values in all columns
print("\nChecking for NaN values in all columns:")
nan_counts = gtha_dgrf.isna().sum()
print(nan_counts)
if nan_counts["popctrpdguid_ctrpoppidugd"] > 0:
    print(f"Note: {nan_counts['popctrpdguid_ctrpoppidugd']} NaN values found in popctrpdguid_ctrpoppidugd (this is expected for some records).")

# Check for NULL values in NOT NULL columns
not_null_columns = ["prdguid_pridugd", "cddguid_dridugd", "dbdguid_ididugd"]
print("\nChecking for NULL values in NOT NULL columns:")
not_null_nan_counts = gtha_dgrf[not_null_columns].isna().sum()
print(not_null_nan_counts)
if not_null_nan_counts.sum() > 0:
    print("Warning: NULL values found in NOT NULL columns. Dropping rows with NULLs in these columns.")
    gtha_dgrf = gtha_dgrf.dropna(subset=not_null_columns)

# Check for duplicate primary keys
print("\nChecking for duplicate primary keys:")
duplicates = gtha_dgrf["dbdguid_ididugd"].duplicated().sum()
print(f"Number of duplicate dbdguid_ididugd values: {duplicates}")
if duplicates > 0:
    print("Removing duplicates...")
    gtha_dgrf = gtha_dgrf.drop_duplicates(subset=["dbdguid_ididugd"])

# Store filtered data in PostgreSQL
try:
    gtha_dgrf.to_sql(
        name="dgrf_ontario_gtha",
        schema="census2021",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )
    print(f"Stored {len(gtha_dgrf)} records in census2021.dgrf_ontario_gtha")
except Exception as e:
    print(f"Error during to_sql: {e}")

# Verify insertion
with engine.connect() as conn:
    count = conn.execute(text("SELECT COUNT(*) FROM census2021.dgrf_ontario_gtha")).scalar()
    print(f"\nTotal records in census2021.dgrf_ontario_gtha: {count}")
    print("\nFirst 2 rows in table:")
    result = conn.execute(text("SELECT * FROM census2021.dgrf_ontario_gtha LIMIT 2")).fetchall()
    for row in result:
        print(row)
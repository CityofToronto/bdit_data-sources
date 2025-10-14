import pandas as pd
from sqlalchemy import create_engine

# Function to create the database engine using credentials stored in .pgpass
def create_db_pgpass_engine():
    with open(expanduser('~/.pgpass'), 'r') as f:
        host, port, database, user, password = f.read().split(':')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    return engine
engine=create_db_pgpass_engine()


# Define DGRF columns (February 2022 release)
columns = [
    "PRDGUID_PRIDUGD",
    "CDDGUID_DRIDUGD",
    "FEDDGUID_CEFIDUGD",
    "CSDDGUID_SDRIDUGD",
    "ERDGUID_REIDUGD",
    "CARDGUID_RARIDUGD",
    "CCSDGUID_SRUIDUGD",
    "DADGUID_ADIDUGD",
    "DBDGUID_IDIDUGD",
    "ADADGUID_ADAIDUGD",
    "DPLDGUID_LDIDUGD",
    "CMAPDGUID_RMRPIDUGD",
    "CMADGUID_RMRIDUGD",
    "CTDGUID_SRIDUGD",
    "POPCTRPDGUID_CTRPOPPIDUGD",
    "POPCTRDGUID_CTRPOPIDUGD"
]

# Read the DGRF CSV file 
# (Downloaded from https://www12.statcan.gc.ca/census-recensement/alternative_alternatif.cfm?l=eng&dispext=zip&teng=2021_98260004.zip&k=%20%20%20%20%202173&loc=//www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/dguid-idugd/files-fichiers/2021_98260004.zip)
dgrf = pd.read_csv("2021_98260004.csv", dtype=str, names=columns, header=None)

# Filter for Ontario (PRDGUID_PRIDUGD = '2021A000235')
ontario_dgrf = dgrf[dgrf["PRDGUID_PRIDUGD"] == "2021A000235"]

# Filter for GTHA Census Divisions
gtha_cd_dguids = [
    "2021A00033518",  # Durham
    "2021A00033519",  # York
    "2021A00033520",  # Toronto
    "2021A00033521",  # Peel
    "2021A00033524",  # Halton
    "2021A00033525"   # Hamilton
]
gtha_dgrf = ontario_dgrf[ontario_dgrf["CDDGUID_DRIDUGD"].isin(gtha_cd_dguids)]

# Store filtered data in PostgreSQL
gtha_dgrf.to_sql(
    name="dgrf_ontario_gtha",
    schema="census2021",
    con=engine,
    if_exists="append",
    index=False
)

print(f"Stored {len(gtha_dgrf)} records in census2021.dgrf_ontario_gtha")
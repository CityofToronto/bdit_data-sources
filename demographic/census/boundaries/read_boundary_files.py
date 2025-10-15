from pathlib import Path
from os.path import expanduser

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text

# ---------- DB ----------
def create_db_pgpass_engine():
    with open(expanduser("~/.pgpass"), "r") as f:
        host, port, database, user, password = f.read().strip().split(":")
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

engine = create_db_pgpass_engine()

# ---------- SETTINGS ----------
PARENT_DIR = Path("/data/home/rbahreh/bdit_data-sources/demographic/census/boundaries")
TARGET_CDS = {"3520", "3518", "3519", "3521", "3524", "3525"}  # CD codes (4 digits)
TARGET_PRUID = "35"                                            # Ontario
COMMENTS = {
    "census_tract": "Census Tract boundaries (GTHA)",
    "dissemination_area": "Dissemination Area boundaries (GTHA)",
    "dissemination_block": "Dissemination Block boundaries (GTHA)",
}

# ---------- DGRF: CT -> CD(last4) ----------
dgrf = pd.read_sql(
    """
    SELECT ctdguid_sridugd, cddguid_dridugd
    FROM census2021.dgrf_ontario_gtha
    WHERE RIGHT(cddguid_dridugd, 4) IN ('3520','3518','3519','3521','3524','3525')
      AND ctdguid_sridugd IS NOT NULL;
    """,
    engine,
)
dgrf.columns = [c.lower() for c in dgrf.columns]
CT_TO_CD_LAST4 = (
    dgrf.assign(cd_last4=dgrf["cddguid_dridugd"].astype(str).str[-4:])[["ctdguid_sridugd", "cd_last4"]]
    .drop_duplicates(subset=["ctdguid_sridugd"])
    .set_index("ctdguid_sridugd")["cd_last4"]
    .to_dict()
)

def table_name_from_file(name: str):
    n = name.lower()
    if "ct" in n: return "census_tract"
    if "da" in n: return "dissemination_area"
    if "db" in n: return "dissemination_block"
    return None

def cd_series_for(gdf: pd.DataFrame, file_name: str) -> pd.Series:
    """Return a Series of CD codes (4 digits) for filtering."""
    n = file_name.lower()
    if "ct" in n:
        # use whichever CT id exists; most common is dguid
        for col in ["dguid", "ctdguid", "ctuid", "ct_id", "ctcode"]:
            if col in gdf.columns:
                return gdf[col].astype(str).map(CT_TO_CD_LAST4)
        return pd.Series(index=gdf.index, dtype="object")
    if "da" in n:
        for col in ["dauid", "da_uid", "daid"]:
            if col in gdf.columns:
                return gdf[col].astype(str).str[:4]
        # fall back to first 4 digits in DGUID if present
        if "dguid" in gdf.columns:
            return gdf["dguid"].astype(str).str.extract(r"(^\d{4})", expand=False)
        return pd.Series(index=gdf.index, dtype="object")
    if "db" in n:
        for col in ["dbuid", "db_uid", "dbid"]:
            if col in gdf.columns:
                return gdf[col].astype(str).str[:4]
        if "dguid" in gdf.columns:
            return gdf["dguid"].astype(str).str.extract(r"(^\d{4})", expand=False)
        return pd.Series(index=gdf.index, dtype="object")
    return pd.Series(index=gdf.index, dtype="object")

def write_simple(engine, gdf, schema, table, mode):
    """
    Super-simple writer:
    - Creates table with all non-geometry columns as TEXT
    - Adds geom column (Geometry, SRID if known)
    - Inserts rows via ST_GeomFromText
    mode: "replace" or "append"
    """
    if "geometry" not in gdf.columns:
        raise ValueError("No geometry column")

    # SRID if EPSG known
    srid = 0
    try:
        epsg = gdf.crs.to_epsg() if gdf.crs else None
        if epsg: srid = int(epsg)
    except Exception:
        pass

    # columns names (lower + safe chars)
    def clean(c): return c.lower().replace(" ", "_").replace("-", "_").replace(".", "_")
    gdf = gdf.copy()
    gdf.columns = [clean(c) for c in gdf.columns]

    # attributes as TEXT 
    attrs = [c for c in gdf.columns if c != "geometry"]
    df_attrs = gdf[attrs].astype(str).where(pd.notnull(gdf[attrs]), None)

    with engine.begin() as conn:
        if mode == "replace":
            conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{table}"'))
            if attrs:
                cols = ", ".join([f'"{c}" TEXT' for c in attrs])
                conn.execute(text(f'CREATE TABLE "{schema}"."{table}" ({cols});'))
            else:
                conn.execute(text(f'CREATE TABLE "{schema}"."{table}" ();'))
            conn.execute(text(
                f'ALTER TABLE "{schema}"."{table}" ADD COLUMN geom geometry(Geometry, {srid});'
            ))

        # insert rows
        cols_sql = ", ".join([f'"{c}"' for c in attrs] + ['"geom"'])
        params_sql = ", ".join([f':{c}' for c in attrs] + [":geom_wkt", ":srid"])
        ins = text(
            f'INSERT INTO "{schema}"."{table}" ({cols_sql}) '
            f'VALUES ({", ".join([f":{c}" for c in attrs])}, ST_GeomFromText(:geom_wkt, :srid));'
        )

        rows = []
        wkt = gdf.geometry.apply(lambda g: g.wkt if g is not None else None)
        for i in range(len(gdf)):
            r = {c: (None if pd.isna(df_attrs.iloc[i][c]) else df_attrs.iloc[i][c]) for c in attrs}
            r["geom_wkt"] = wkt.iloc[i]
            r["srid"] = srid
            rows.append(r)

        # chunked inserts
        step = 5000
        for j in range(0, len(rows), step):
            conn.execute(ins, rows[j:j+step])

# ---------- MAIN ----------
folders = [p for p in PARENT_DIR.iterdir() if p.is_dir()] if PARENT_DIR.exists() else []
written_mode = {
    "census_tract": None,
    "dissemination_area": None,
    "dissemination_block": None,
}  # tracks replace/append per table
uploaded = []

for folder in folders:
    shp_files = list(folder.glob("*.shp"))
    if not shp_files:
        print(f"No shapefiles in {folder.name}")
        continue

    for shp in shp_files:
        try:
            tname = table_name_from_file(shp.name)
            if tname is None:
                print(f"Skip (unknown type): {shp.name}")
                continue

            gdf = gpd.read_file(shp)
            gdf.columns = [c.lower() for c in gdf.columns]

            # Province filter
            pr_ok = True
            if "pruid" in gdf.columns:
                pr_ok = gdf["pruid"].astype(str) == TARGET_PRUID

            # CD filter (rules by type)
            cd_series = cd_series_for(gdf, shp.name)
            cd_ok = cd_series.isin(TARGET_CDS) if cd_series.notna().any() else False

            keep = gdf[pr_ok & cd_ok]
            print(f"\n{folder.name}/{shp.name}: {len(gdf)} → {len(keep)} after filtering")
            if tname == "census_tract":
                miss = cd_series.isna().sum()
                if miss:
                    print(f"  [warn] {miss} CT features had no DGRF match")

            if keep.empty:
                print("  Skipped (no rows).")
                continue

            mode = "replace" if written_mode[tname] is None else "append"
            write_simple(engine, keep, "census2021", tname, mode)
            written_mode[tname] = "append"

            # Add comment
            with engine.begin() as conn:
                conn.execute(
                    text(f'COMMENT ON TABLE "census2021"."{tname}" IS :c'),
                    {"c": COMMENTS[tname]},
                )

            uploaded.append((tname, len(keep)))
            print(f"  Saved → census2021.{tname} ({mode})")

        except Exception as e:
            print(f"Error processing {shp}: {e}")

# ---------- SUMMARY ----------
print("\nSummary:")
for t, n in uploaded:
    print(f"  census2021.{t}: {n} records")
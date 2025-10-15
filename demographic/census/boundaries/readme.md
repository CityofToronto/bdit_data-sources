# 2021 Census Boundary Files

## Overview
The boundary files downloaded from  were downloaded from the Statistics Canada website:
- URL: [https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/index2021-eng.cfm?year=21](https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/index2021-eng.cfm?year=21). These files include geospatial data for administrative and statistical geographic areas: Census Tracts (CT), Dissemination Areas (DA), and Dissemination Blocks (DB) in the Greater Toronto and Hamilton Area (GTHA).

The files used here are shapefiles (`.shp`) stored in the directory:
```
K:\tra\GM Office\Big Data Group\Work\Projects\2025-TEOZ\census2021
```
The data were filtered to include only areas within specific Census Divisions (CDs) in Ontario (PRUID = "35") with CD codes: 3520, 3518, 3519, 3521, 3524, 3525.
and were stored in `census2021` schema, in tables:
   - `census_tract`
   - `dissemination_area`
   - `dissemination_block`
**Note:** I used a mapping table (`dgrf_ontario_gtha`) to associate Census Tracts with Census Divisions.

Date created: October 15, 2025
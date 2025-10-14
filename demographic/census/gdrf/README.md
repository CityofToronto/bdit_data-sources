# 2021 Census DGRF Ontario GTHA Table

## Overview
 `census2021.dgrf_ontario_gtha` table contains the 2021 Dissemination Geographies Relationship File (DGRF) data filtered for Ontario (PRDGUID_PRIDUGD = '2021A000235') and the Greater Toronto and Hamilton Area (GTHA) census divisions (Durham, York, Toronto, Peel, Halton, Hamilton). 

The data is sourced from Statistics Canada’s 2021 Census DGRF file, available at:
[2021_98260004.zip](https://www12.statcan.gc.ca/census-recensement/alternative_alternatif.cfm?l=eng&dispext=zip&teng=2021_98260004.zip&k=%20%20%20%20%202173&loc=//www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/dguid-idugd/files-fichiers/2021_98260004.zip).

The table Stores geographic relationships for dissemination blocks in Ontario’s GTHA region, linking dissemination blocks to higher-level geographies (e.g., census divisions, census subdivisions, population centres).

### Table Schema
The table has 16 columns, all defined as `VARCHAR(21)` to store DGUIDs (Dissemination Geography Unique Identifiers). The columns are named in lowercase to match the database schema.

| Column Name                     | Description                                              | Constraints       | Notes                                                                 |
|---------------------------------|----------------------------------------------------------|-------------------|----------------------------------------------------------------------|
| prdguid_pridugd                 | Province/Territory DGUID                                 | NOT NULL          | Always '2021A000235' for Ontario in this table.                      |
| cddguid_dridugd                 | Census Division DGUID                                    | NOT NULL          | GTHA divisions (e.g., '2021A00033518' for Durham).                   |
| feddguid_cefidugd               | Federal Electoral District DGUID                         |                   |                                                                      |
| csddguid_sdridugd               | Census Subdivision DGUID                                 |                   |                                                                      |
| erdguid_reidugd                 | Economic Region DGUID                                    |                   |                                                                      |
| cardguid_raridugd               | Census Agricultural Region DGUID                         |                   |                                                                      |
| ccsdguid_sruidugd               | Consolidated Census Subdivision DGUID                   |                   |                                                                      |
| dadguid_adidugd                 | Dissemination Area DGUID                                 |                   |                                                                      |
| dbdguid_ididugd                 | Dissemination Block DGUID                                | NOT NULL, PRIMARY KEY | Unique identifier for each dissemination block.                      |
| adadguid_adaidugd               | Aggregate Dissemination Area DGUID                       |                   |                                                                      |
| dpldguid_ldidugd                | Designated Place DGUID                                   |                   | Often NULL for GTHA records.                                         |
| cmapdguid_rmrpidugd             | Census Metropolitan Area Previous DGUID                  |                   | Often NULL for GTHA records.                                         |
| cmadguid_rmridugd               | Census Metropolitan Area DGUID                           |                   | May be NULL for some records.                                        |
| ctdguid_sridugd                 | Census Tract DGUID                                       |                   | May be NULL for some records.                                        |
| popctrpdguid_ctrpoppidugd       | Population Centre Previous DGUID                         |                   | Often NULL for dissemination blocks not in population centres.        |
| popctrdguid_ctrpopidugd         | Population Centre DGUID                                  |                   | May be NULL for some records.                                        |

- **Indexes**
  - `idx_dgrf_ontario_gtha_cd`: On `cddguid_dridugd` for filtering by census division.
  - `idx_dgrf_ontario_gtha_csd`: On `csddguid_sdridugd` for filtering by census subdivision.
  - `idx_dgrf_ontario_gtha_da`: On `dadguid_adidugd` for filtering by dissemination area.

- **Record Count**: Approximately 42,928 records for GTHA dissemination blocks (based on filtering results).
- **NULL Values**: Some columns, such as `popctrpdguid_ctrpoppidugd`, `popctrdguid_ctrpopidugd`, `dpldguid_ldidugd`, `cmapdguid_rmrpidugd`, `cmadguid_rmridugd`, and `ctdguid_sridugd`, may contain `NULL` values. This is expected, as not all dissemination blocks map to these geographic levels (e.g., population centres or census tracts).
- **Primary Key**: `dbdguid_ididugd` is unique and used as the primary key.

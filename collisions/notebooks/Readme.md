# Collisions Pipeline Diagnostic Notebooks

This folder houses various investigations and experiments related to the
collisions pipeline.

- `20210422_mva_check.ipynb` - compares the MVA XML files on `\\tssrv7\CollisionsProcessed\AtSceneXmlBackups` with collisions in `collisions.events` with ACCNB that look like they were derived from TPS collisions.
- `20210503_mva_check2.ipynb` - compares the XML files with collisions in FLOWQA's `TRAFFIC.ACC` and `TRAFFIC.ACC_ARCHIVE`. Confirms that many of the XML files missing in `collisions.events` were diverted to `ACC_ARCHIVE` (because they were deemed to be on private property).
- `20210517_mva_tps_check.ipynb` - three-way check between a dump of collisions from TPS's Versadex database, the MVA XML files on our network folder, and the FLOWQA `TRAFFIC.ACC` and `TRAFFIC.ACC_ARCHIVE` tables. We found discrepancies between any two out of the three datasets.

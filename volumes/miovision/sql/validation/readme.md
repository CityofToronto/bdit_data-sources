- [`miovision_validation.spectrum_studies` (table)](#miovision_validationspectrum_studies-table)
- [`miovision_validation.mio_spec_processed_counts` (table)](#miovision_validationmio_spec_processed_counts-table)
- [Validation Flow Chart](#validation-flow-chart)

### `miovision_validation.spectrum_studies` (table)
Spectrum studies at Miovision intersections. Updated daily by `miovision_validation` Airflow DAG.
- Vehicle approach counts are stored in order to identify when a spectrum study has been updated (by comparing to what's in `traffic.tmc_summary_stats`)

Approx row count:                    640
| Column Name      | Data Type   | Sample     | Comments   |
|------------------|-------------|------------|------------|
| intersection_uid | integer     | 20         |            |
| int_id           | integer     | 13466630   |            |
| count_date       | date        | 2019-03-02 |            |
| count_id         | bigint      | 38407      |            |
| processed        | boolean     | True       |            |
| count_veh_n_appr | numeric     | 4571.0     |            |
| count_veh_s_appr | numeric     | 3819.0     |            |
| count_veh_e_appr | numeric     | 828.0      |            |
| count_veh_w_appr | numeric     | 532.0      |            |

### `miovision_validation.mio_spec_processed_counts` (table)

Stores Miovision and Spectrum study results in order to make downstream calculations quicker. Updated daily by `miovision_validation` Airflow DAG.

Approx row count:            1,195,300
| Column Name          | Data Type                   | Sample                               | Comments   |
|----------------------|-----------------------------|--------------------------------------|------------|
| intersection_uid     | integer                     | 1                                    |            |
| count_id             | bigint                      | 38677                                |            |
| count_date           | date                        | 2019-04-09                           |            |
| datetime_bin         | timestamp without time zone | 2019-04-09 13:00:00                  |            |
| spec_movements       | text[]                      | ['s_bus_t', 's_cars_t', 's_truck_t'] |            |
| leg                  | text                        | S                                    |            |
| spec_class           | text                        | vehicle_all                          |            |
| movement_name        | text                        | Thru                                 |            |
| spec_count           | numeric                     | 101.0                                |            |
| miovision_api_volume | numeric                     | 102.0                                |            |
| bin_error            | numeric                     | 0.0099                               |            |
| classification_uids  | integer[]                   | [1, 3, 4, 5, 8]                      |            |
| movement_uids        | integer[]                   | [1]                                  |            |

### Validation Flow Chart

This flow chart shows the data flows involved in the Miovision validation pipeline.
The `Data Collection spectrum_miovision_validation views` which implement sensor acceptance criteria are stored [here](https://github.com/Toronto-Big-Data-Innovation-Team/data_validation/tree/main/miovision).

```mermaid
%%{init: {'theme': 'neutral', 'flowchart': {'defaultRenderer': 'elk'}}}%%
flowchart TD

    subgraph "Identify Spectrum Studies"
        insert_spectrum_studies@{ shape: braces, label: "miovision_validation.insert_spectrum_studies()"}
        traffic.tmc_metadata@{ shape: card} --> insert_spectrum_studies
        intersections@{label: "miovision_api.intersections", shape: card}
        insert_spectrum_studies -->|Log studies| miovision_validation.spectrum_studies@{ shape: card}
    end

    subgraph "Miovision Validation"

        v15@{ shape: procs, label: "miovision_api.volumes_15min_mvt_unfiltered" }
        insert_processed_counts@{ shape: braces, label: "miovision_validation.insert_processed_counts()"}
        
        insert_processed_counts -->|Cache counts to improve query speeds| miovision_validation.mio_spec_processed_counts@{ shape: card}
        v15 --> insert_processed_counts
        intersections --> insert_spectrum_studies
        miovision_validation.spectrum_studies --> insert_processed_counts
        miovision_validation.spec_class_move_map@{ shape: card} --> insert_processed_counts
        insert_processed_counts ----> |Log if data| miovision_validation.spectrum_studies
        miovision_validation.mio_spec_processed_counts --> DC_Views@{ shape: manual, label: "Data Collection spectrum_miovision_validation views" }
        DC_Views --> miovision_validation.valid_legs_view@{ shape: manual} --> |Cache to improve speeds and reduce dependencies| miovision_validation.valid_legs@{ shape: card}
        DC_Views --> miovision_validation.valid_intersections_view@{ shape: manual} --> |Cache to improve speeds and reduce dependencies| miovision_validation.valid_intersections@{ shape: card}
        miovision_validation.spectrum_studies -->|Anti-join already processed studies| insert_spectrum_studies
        traffic.tmc_study_data@{ shape: card} --> insert_processed_counts
        
    end

    subgraph Legend
        Partition@{ shape: procs, label: "Partitioned Tables" }
        Table@{ shape: card}
        Function@{ shape: braces, label: Function}
        View@{ shape: manual}        
        Partition --> Function --> Table
    end
```

Format for all traffic volume tables should mimic the following:

**Turning Movement Counts (TMCs)**

| Column Name | Type | Description |
|-------------|------|-------------|
| int_id  | Integer | Signalized intersection identifier linking back to the City's Centreline Intersections dataset. |
| px  | Integer | Signalized intersection identifier linking back to the City's Traffic Signals dataset. |
| location | Text | Textual description of street segment location and direction. |
| class_type | Text | Textual description of the modal class (Cyclists, Pedestrians, or Vehicles). |
| leg | Text | Textual description of the specific leg of the intersection (N, S, E, or W). |
| movement | Text | Textual description of the specific movement observed (Through, Left, or Right). If NULL, record represents volumes observed on the crosswalk. |
| datetime_bin | Timestamp | Start of 15-minute period. |
| volume_15min | Integer | Total volume observed over the 15-minute period. |


**Automatic Traffic Recorders (ATRs)**

| Column Name | Type | Description |
|-------------|------|-------------|
| centreline_id  | Integer | Street segment identifier linking back to the City's Centreline. | 
| direction | Text | Cardinal direction of vehicle travel along street segment. | 
| location | Text | Textual description of street segment location and direction. | 
| class_type | Text | Textual description of the modal class (Cyclists, Pedestrians, or Vehicles). |
| datetime_bin | Timestamp | Start of specific period. |
| volume_15min | Integer | Total volume observed over the specific period. |

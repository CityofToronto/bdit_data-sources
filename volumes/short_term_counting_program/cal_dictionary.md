## What is `TRAFFIC.CAL`?

This is an old FLOW table (`TRAFFIC.CAL`) in the legacy Oracle database. It appears to contain Turning Movement Count (TMC) summary statistics for AM peak, PM peak, off peak, and 8-hour totals for a TMC study. These include by direction, mode, and movements.

## Column List

Field Name|Type|Description
----------|----|----------
ID|Autonumber|Autonumber function
COUNT_INFO_ID|number|ID number linked to countinfomics table containing count dates
COMMENT|Text|may contain comment pertaining to count
AM_PK_HR|Date/Time|A.M. Peak Hour - start time displayed
PM_PK_HR|Date/Time|P.M. Peak Hour - start time displayed
OFF_PK_HR|Date/Time|field is not used
AM_2_HR|Date/Time|field is not used
PM_2_HR|Date/Time|field is not used
AM_NB_X|number|*A.M. Peak Hr - N/B exits (sum of cars, trucks, bus for S_XXX_T + W_XXX_L+E_XXX_R) from DET_XXXX*
AM_NB_T|number|*A.M. Peak Hr - N/B Thru (sum of cars, trucks, bus S_XXX_T ) from DET_XXXX*
AM_NB_R|number|*A.M. Peak Hr - N/B right turns (sum of cars, trucks, bus| for S_XXX_R from DET_XXXX*
AM_NB_L|number|*A.M. Peak Hr - N/B left turns (sum of cars, trucks, bus for S_XXX_L from DET_XXXX*
AM_NB_B|number|A.M. Peak Hr - N/B bikes = S_BIKES from DET_XXXX
AM_NB_O|number|A.M. Peak Hr - N/B OTHER = S_OTHER from DET_XXXX
AM_NS_P|number|A.M. Peak Hr - Pedestrians crossing on North Side ( N_PEDS from DET_XXXX)
AM_SB_X|number|*A.M. Peak Hr - S/B exits (sum of cars, trucks, bus for N_XXX_T + E_XXX_L+ W_XXX_R) from DET_XXXX*
AM_SB_T|number|*A.M. Peak Hr - S/B Thru (sum of cars, trucks, bus) N_XXX_T  from DET_XXXX*
AM_SB_R|number|*A.M. Peak Hr - S/B right turns (sum of cars, trucks, bus| for N_XXX_R from DET_XXXX*
AM_SB_L|number|*A.M. Peak Hr - S/B left turns (sum of cars, trucks, bus for N_XXX_L from DET_XXXX*
AM_SB_B|number|A.M. Peak Hr - S/B bikes = N_BIKES from DET_XXXX
AM_SB_O|number|A.M. Peak Hr - S/B OTHER = N_OTHER from DET_XXXX
AM_SS_P|number|A.M. Peak Hr - Pedestrians crossing on South Side ( S_PEDS from DET_XXXX)
AM_WB_X|number|*A.M. Peak Hr - W/B exits (sum of cars, trucks, bus for E_XXX_T + S_XXX_L+N_XXX_R) from DET_XXXX*
AM_WB_T|number|*A.M. Peak Hr - W/B Thru (sum of cars, trucks, bus E_XXX_T ) from DET_XXXX*
AM_WB_R|number|*A.M. Peak Hr - W/B right turns (sum of cars, trucks, bus| for E_XXX_R from DET_XXXX*
AM_WB_L|number|*A.M. Peak Hr - W/B left turns (sum of cars, trucks, bus for E_XXX_L from DET_XXXX*
AM_WB_B|number|A.M. Peak Hr - W/B bikes = E_BIKES from DET_XXXX
AM_WB_O|number|A.M. Peak Hr - W/B OTHER = E_OTHER from DET_XXXX
AM_WS_P|number|A.M. Peak Hr - Pedestrians crossing on West Side ( W_PEDS from DET_XXXX)
AM_EB_X|number|*A.M. Peak Hr - E/B exits (sum of cars, trucks, bus for W_XXX_T + N_XXX_L+ S_XXX_R) from DET_XXXX*
AM_EB_T|number|*A.M. Peak Hr - E/B Thru (sum of cars, trucks, bus) W_XXX_T  from DET_XXXX*
AM_EB_R|number|*A.M. Peak Hr - E/B right turns (sum of cars, trucks, bus| for W_XXX_R from DET_XXXX*
AM_EB_L|number|*A.M. Peak Hr - E/B left turns (sum of cars, trucks, bus for W_XXX_L from DET_XXXX*
AM_EB_B|number|A.M. Peak Hr - E/B bikes = W_BIKES from DET_XXXX
AM_EB_O|number|A.M. Peak Hr - E/B OTHER = W_OTHER from DET_XXXX
AM_ES_P|number|A.M. Peak Hr - Pedestrians crossing on East Side ( E_PEDS from DET_XXXX)
PM_NB_X|number|*P.M. Peak Hr - N/B exits (sum of cars, trucks, bus for S_XXX_T + W_XXX_L+E_XXX_R) from DET_XXXX*
PM_NB_T|number|*P.M. Peak Hr - N/B Thru (sum of cars, trucks, bus S_XXX_T ) from DET_XXXX*
PM_NB_R|number|*P.M. Peak Hr - N/B right turns (sum of cars, trucks, bus| for S_XXX_R from DET_XXXX*
PM_NB_L|number|*P.M. Peak Hr - N/B left turns (sum of cars, trucks, bus for S_XXX_L from DET_XXXX*
PM_NB_B|number|P.M. Peak Hr - N/B bikes = S_BIKES from DET_XXXX
PM_NB_O|number|P.M. Peak Hr - N/B OTHER = S_OTHER from DET_XXXX
PM_NS_P|number|P.M. Peak Hr - Pedestrians crossing on North Side ( N_PEDS from DET_XXXX)
PM_SB_X|number|*P.M. Peak Hr - S/B exits (sum of cars,trucks,bus for N_XXX_T + E_XXX_L+ W_XXX_R) from DET_XXXX*
PM_SB_T|number|*P.M. Peak Hr - S/B Thru (sum of cars,trucks,bus) N_XXX_T  from DET_XXXX*
PM_SB_R|number|*P.M. Peak Hr - S/B right turns (sum of cars,trucks,bus for N_XXX_R from DET_XXXX*
PM_SB_L|number|*P.M. Peak Hr - S/B left turns (sum of cars,trucks,bus for N_XXX_L from DET_XXXX*
PM_SB_B|number|P.M. Peak Hr - S/B bikes = N_BIKES from DET_XXXX
PM_SB_O|number|P.M. Peak Hr - S/B OTHER = N_OTHER from DET_XXXX
PM_SS_P|number|P.M. Peak Hr - Pedestrians crossing on South Side ( S_PEDS from DET_XXXX)
PM_WB_X|number|*P.M. Peak Hr - W/B exits (sum of cars,trucks,bus for E_XXX_T + S_XXX_L+N_XXX_R) from DET_XXXX*
PM_WB_T|number|*P.M. Peak Hr - W/B Thru (sum of cars,trucks,bus E_XXX_T ) from DET_XXXX*
PM_WB_R|number|*P.M. Peak Hr - W/B right turns (sum of cars,trucks,bus for E_XXX_R from DET_XXXX*
PM_WB_L|number|*P.M. Peak Hr - W/B left turns (sum of cars,trucks,bus for E_XXX_L from DET_XXXX*
PM_WB_B|number|P.M. Peak Hr - W/B bikes = E_BIKES from DET_XXXX
PM_WB_O|number|P.M. Peak Hr - W/B OTHER = E_OTHER from DET_XXXX
PM_WS_P|number|P.M. Peak Hr - Pedestrians crossing on West Side ( W_PEDS from DET_XXXX)
PM_EB_X|number|*P.M. Peak Hr - E/B exits (sum of cars, trucks, bus for W_XXX_T + N_XXX_L+ S_XXX_R) from DET_XXXX*
PM_EB_T|number|*P.M. Peak Hr - E/B Thru (sum of cars, trucks, bus) W_XXX_T  from DET_XXXX*
PM_EB_R|number|*P.M. Peak Hr - E/B right turns (sum of cars, trucks, bus| for W_XXX_R from DET_XXXX*
PM_EB_L|number|*P.M. Peak Hr - E/B left turns (sum of cars, trucks, bus for W_XXX_L from DET_XXXX*
PM_EB_B|number|P.M. Peak Hr - E/B bikes = W_BIKES from DET_XXXX
PM_EB_O|number|P.M. Peak Hr - E/B OTHER = W_OTHER from DET_XXXX
PM_ES_P|number|P.M. Peak Hr - Pedestrians crossing on East Side ( E_PEDS from DET_XXXX)
OF_NB_X|number|*OFF Peak Hr - N/B exits (sum of cars, trucks, bus for S_XXX_T + W_XXX_L+E_XXX_R) from DET_XXXX*
OF_NB_T|number|*OFF Peak Hr - N/B Thru (sum of cars, trucks, bus S_XXX_T ) from DET_XXXX*
OF_NB_R|number|*OFF Peak Hr - N/B right turns (sum of cars, trucks, bus| for S_XXX_R from DET_XXXX*
OF_NB_L|number|*OFF Peak Hr - N/B left turns (sum of cars, trucks, bus for S_XXX_L from DET_XXXX*
OF_NB_B|number|OFF Peak Hr - N/B bikes = S_BIKES from DET_XXXX
OF_NB_O|number|OFF Peak Hr - N/B OTHER = S_OTHER from DET_XXXX
OF_NS_P|number|OFF Peak Hr - Pedestrians crossing on North Side ( N_PEDS from DET_XXXX)
OF_SB_X|number|*OFF Peak Hr - S/B exits (sum of cars, trucks, bus for N_XXX_T + E_XXX_L+ W_XXX_R) from DET_XXXX*
OF_SB_T|number|*OFF Peak Hr - S/B Thru (sum of cars, trucks, bus) N_XXX_T  from DET_XXXX*
OF_SB_R|number|*OFF Peak Hr - S/B right turns (sum of cars, trucks, bus| for N_XXX_R from DET_XXXX*
OF_SB_L|number|*OFF Peak Hr - S/B left turns (sum of cars, trucks, bus for N_XXX_L from DET_XXXX*
OF_SB_B|number|OFF Peak Hr - S/B bikes = N_BIKES from DET_XXXX
OF_SB_O|number|OFF Peak Hr - S/B OTHER = N_OTHER from DET_XXXX
OF_SS_P|number|OFF Peak Hr - Pedestrians crossing on South Side ( S_PEDS from DET_XXXX)
OF_WB_X|number|*OFF Peak Hr - W/B exits (sum of cars, trucks, bus for E_XXX_T + S_XXX_L+N_XXX_R) from DET_XXXX*
OF_WB_TS|number|*OFF Peak Hr - W/B Thru (sum of cars, trucks, bus E_XXX_T ) from DET_XXXX*
OF_WB_R|number|*OFF Peak Hr - W/B right turns (sum of cars, trucks, bus| for E_XXX_R from DET_XXXX*
OF_WB_L|number|*OFF Peak Hr - W/B left turns (sum of cars, trucks, bus for E_XXX_L from DET_XXXX*
OF_WB_B|number|OFF Peak Hr - W/B bikes = E_BIKES from DET_XXXX
OF_WB_O|number|OFF Peak Hr - W/B OTHER = E_OTHER from DET_XXXX
OF_WS_P|number|OFF Peak Hr - Pedestrians crossing on West Side ( W_PEDS from DET_XXXX)
OF_EB_X|number|*OFF Peak Hr - E/B exits (sum of cars, trucks, bus for W_XXX_T + N_XXX_L+ S_XXX_R) from DET_XXXX*
OF_EB_T|number|*OFF Peak Hr - E/B Thru (sum of cars, trucks, bus) W_XXX_T  from DET_XXXX*
OF_EB_R|number|*OFF Peak Hr - E/B right turns (sum of cars, trucks, bus| for W_XXX_R from DET_XXXX*
OF_EB_L|number|*OFF Peak Hr - E/B left turns (sum of cars, trucks, bus for W_XXX_L from DET_XXXX*
OF_EB_B|number|OFF Peak Hr - E/B bikes = W_BIKES from DET_XXXX
OF_EB_O|number|OFF Peak Hr - E/B OTHER = W_OTHER from DET_XXXX
OF_ES_P|number|OFF Peak Hr - Pedestrians crossing on East Side ( E_PEDS from DET_XXXX)
TL_NB_X|number|*TOTAL 8 HR  - N/B exits (sum of cars, trucks, bus for S_XXX_T + W_XXX_L+E_XXX_R) from DET_XXXX*
TL_NB_T|number|*TOTAL 8 HR - N/B Thru (sum of cars, trucks, bus S_XXX_T ) from DET_XXXX*
TL_NB_R|number|*TOTAL 8 HR - N/B right turns (sum of cars, trucks, bus| for S_XXX_R from DET_XXXX*
TL_NB_L|number|*TOTAL 8 HR - N/B left turns (sum of cars, trucks, bus for S_XXX_L from DET_XXXX*
TL_NB_B|number|TOTAL 8 HR - N/B bikes = S_BIKES from DET_XXXX
TL_NB_O|number|TOTAL 8 HR - N/B OTHER = S_OTHER from DET_XXXX
TL_NS_P|number|TOTAL 8 HR - Pedestrians crossing on North Side ( N_PEDS from DET_XXXX)
TL_SB_X|number|*TOTAL 8 HR - S/B exits (sum of cars, trucks, bus for N_XXX_T + E_XXX_L+ W_XXX_R) from DET_XXXX*
TL_SB_T|number|*TOTAL 8 HR - S/B Thru (sum of cars, trucks, bus) N_XXX_T  from DET_XXXX*
TL_SB_R|number|*TOTAL 8 HR - S/B right turns (sum of cars, trucks, bus| for N_XXX_R from DET_XXXX*
TL_SB_L|number|*TOTAL 8 HR - S/B left turns (sum of cars, trucks, bus for N_XXX_L from DET_XXXX*
TL_SB_B|number|TOTAL 8 HR - S/B bikes = N_BIKES from DET_XXXX
TL_SB_O|number|TOTAL 8 HR - S/B OTHER = N_OTHER from DET_XXXX
TL_SS_P|number|TOTAL 8 HR - Pedestrians crossing on South Side ( S_PEDS from DET_XXXX)
TL_WB_X|number|*TOTAL 8 HR - W/B exits (sum of cars, trucks, bus for E_XXX_T + S_XXX_L+N_XXX_R) from DET_XXXX*
TL_WB_TS|number|*TOTAL 8 HR - W/B Thru (sum of cars, trucks, bus E_XXX_T ) from DET_XXXX*
TL_WB_R|number|*TOTAL 8 HR - W/B right turns (sum of cars, trucks, bus| for E_XXX_R from DET_XXXX*
TL_WB_L|number|*TOTAL 8 HR - W/B left turns (sum of cars, trucks, bus for E_XXX_L from DET_XXXX*
TL_WB_B|number|TOTAL 8 HR - W/B bikes = E_BIKES from DET_XXXX
TL_WB_O|number|TOTAL 8 HR - W/B OTHER = E_OTHER from DET_XXXX
TL_WS_P|number|TOTAL 8 HR - Pedestrians crossing on West Side ( W_PEDS from DET_XXXX)
TL_EB_X|number|*TOTAL 8 HR - E/B exits (sum of cars, trucks, bus for W_XXX_T + N_XXX_L+ S_XXX_R) from DET_XXXX*
TL_EB_T|number|*TOTAL 8 HR - E/B Thru (sum of cars, trucks, bus) W_XXX_T  from DET_XXXX*
TL_EB_R|number|*TOTAL 8 HR - E/B right turns (sum of cars, trucks, bus| for W_XXX_R from DET_XXXX*
TL_EB_L|number|*TOTAL 8 HR - E/B left turns (sum of cars, trucks, bus for W_XXX_L from DET_XXXX*
TL_EB_B|number|TOTAL 8 HR - E/B bikes = W_BIKES from DET_XXXX
TL_EB_O|number|TOTAL 8 HR - E/B OTHER = W_OTHER from DET_XXXX
TL_ES_P|number|TOTAL 8 HR - Pedestrians crossing on East Side ( E_PEDS from DET_XXXX)

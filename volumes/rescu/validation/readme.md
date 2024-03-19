This PR accomplishes identification of network wide and individual detector outages.

1) **volumes/rescu/validation/identify_sensors_to_repair_i0617.sql**
   - Create a list of detectors classified as good/bad/inactive to map for geographic distribution.  
   - Uses table 'network_outages' to elimante dates where all detectors are inactive from the denominator of up-time to get more realistic numbers.  
   - Create a summary table of detectors with different stats: total_volume, bins_active_percent, bins_active, last_active.  
   - Includes summary stats over a list of dates for active sensors and 'last active date' for any sensors not active during this period for completeness.  
  
2) **Mapped `gwolofs.i0617_rescu_sensor_eval` in QGIS to visually inspect and determine sensors to repair:**
![Alt text](identify_detectors_for_repair.png)

3) **volumes/rescu/validation/evaluate_rescu_network.ipynb**  
   A short ipynb to explore the results of the above queries. 



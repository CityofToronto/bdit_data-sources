/*
   Purpose:
   Creates a table that calculates the total annual 
   Vehicle Kilometres Traveled (VKT) proxy for each segment 
   using two weighting approaches:
     1. Direct sample size (volume-based VKT)
     2. Square root of sample size (sqrt-weighted VKT)
   
   - Input tables:
       • congestion.network_links_24_4
          provides segment_id and link_dir
       • sborjia.here_annual_sample_2024_length
          provides sample_size and length per link_dir
   
   - Output columns:
       • segment_id
       • vkt_km          = SUM(sample_size × length) / 1000
       • sqrt_vkt_km     = SUM(SQRT(sample_size) × length) / 1000
   */

CREATE TABLE sborjia.segment_here_vkt_2024 AS
SELECT 
    nl.segment_id,
    -- Total VKT per segment in kilometers (volume-based)
    SUM(a.sample_size * a.length) / 1000.0 AS vkt_km,
    -- Total VKT per segment in kilometers (square root of volume as weight)
    SUM(SQRT(a.sample_size) * a.length) / 1000.0 AS sqrt_vkt_km
FROM congestion.network_links_24_4 AS nl
JOIN sborjia.here_annual_sample_2024_length AS a
    ON nl.link_dir = a.link_dir
GROUP BY nl.segment_id;

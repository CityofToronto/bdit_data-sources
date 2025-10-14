-- Drop table if it exists to ensure a clean slate
DROP TABLE IF EXISTS census2021.dgrf_ontario_gtha;

-- Create table for DGRF data, filtered for Ontario and GTHA
CREATE TABLE census2021.dgrf_ontario_gtha (
    prdguid_pridugd VARCHAR(21) NOT NULL,
    cddguid_dridugd VARCHAR(21) NOT NULL,
    feddguid_cefidugd VARCHAR(21),
    csddguid_sdridugd VARCHAR(21),
    erdguid_reidugd VARCHAR(21),
    cardguid_raridugd VARCHAR(21),
    ccsdguid_sruidugd VARCHAR(21),
    dadguid_adidugd VARCHAR(21),
    dbdguid_ididugd VARCHAR(21) NOT NULL,
    adadguid_adaidugd VARCHAR(21),
    dpldguid_ldidugd VARCHAR(21),
    cmapdguid_rmrpidugd VARCHAR(21),
    cmadguid_rmridugd VARCHAR(21),
    ctdguid_sridugd VARCHAR(21),
    popctrpdguid_ctrpoppidugd VARCHAR(21),
    popctrdguid_ctrpopidugd VARCHAR(21),
    PRIMARY KEY (dbdguid_ididugd)
);

-- Add indexes for efficient filtering and joining
CREATE INDEX idx_dgrf_ontario_gtha_cd ON census2021.dgrf_ontario_gtha (cddguid_dridugd);
CREATE INDEX idx_dgrf_ontario_gtha_csd ON census2021.dgrf_ontario_gtha (csddguid_sdridugd);
CREATE INDEX idx_dgrf_ontario_gtha_da ON census2021.dgrf_ontario_gtha (dadguid_adidugd);

-- Add comment for documentation
COMMENT ON TABLE census2021.dgrf_ontario_gtha IS '2021 Census DGRF data filtered for Ontario and GTHA Census Divisions (Durham, York, Toronto, Peel, Halton, Hamilton).';
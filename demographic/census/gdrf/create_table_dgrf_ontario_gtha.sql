-- Create table for DGRF data, filtered for Ontario and GTHA
-- -- 2021 Dissemination Geographies Relationship File (DGRF)
CREATE TABLE census2021.dgrf_ontario_gtha (
    PRDGUID_PRIDUGD VARCHAR(21) NOT NULL,
    CDDGUID_DRIDUGD VARCHAR(21) NOT NULL,
    FEDDGUID_CEFIDUGD VARCHAR(21),
    CSDDGUID_SDRIDUGD VARCHAR(21),
    ERDGUID_REIDUGD VARCHAR(21),
    CARDGUID_RARIDUGD VARCHAR(21),
    CCSDGUID_SRUIDUGD VARCHAR(21),
    DADGUID_ADIDUGD VARCHAR(21),
    DBDGUID_IDIDUGD VARCHAR(21) NOT NULL,
    ADADGUID_ADAIDUGD VARCHAR(21),
    DPLDGUID_LDIDUGD VARCHAR(21),
    CMAPDGUID_RMRPIDUGD VARCHAR(21),
    CMADGUID_RMRIDUGD VARCHAR(21),
    CTDGUID_SRIDUGD VARCHAR(21),
    POPCTRPDGUID_CTRPOPPIDUGD VARCHAR(21),
    POPCTRDGUID_CTRPOPIDUGD VARCHAR(21),
    PRIMARY KEY (DBDGUID_IDIDUGD)
);

-- Add indexes for efficient filtering and joining
CREATE INDEX idx_dgrf_ontario_gtha_pr ON census2021.dgrf_ontario_gtha (PRDGUID_PRIDUGD);
CREATE INDEX idx_dgrf_ontario_gtha_cd ON census2021.dgrf_ontario_gtha (CDDGUID_DRIDUGD);
CREATE INDEX idx_dgrf_ontario_gtha_csd ON census2021.dgrf_ontario_gtha (CSDDGUID_SDRIDUGD);
CREATE INDEX idx_dgrf_ontario_gtha_da ON census2021.dgrf_ontario_gtha (DADGUID_ADIDUGD);

-- Add comment for documentation
COMMENT ON TABLE census2021.dgrf_ontario_gtha IS '2021 Census DGRF data filtered for Ontario and GTHA Census Divisions (Durham, York, Toronto, Peel, Halton, Hamilton).';
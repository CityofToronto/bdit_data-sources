UPDATE ecocounter.flows
SET flow_geom = ST_Reverse(flow_geom)
WHERE flow_id IN (353534018, 353534020,353534008,353534010)
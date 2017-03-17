SELECT link_id, st_name, feat_id, st_langcd, num_stnmes, st_nm_pref, st_typ_bef, st_nm_base, st_nm_suff, st_typ_aft, st_typ_att, addr_type, l_refaddr, l_nrefaddr, l_addrsch, l_addrform, r_refaddr, r_nrefaddr, r_addrsch, r_addrform, ref_in_id, nref_in_id, n_shapepnt, func_class, speed_cat, fr_spd_lim, to_spd_lim, to_lanes, from_lanes, enh_geom, lane_cat, divider, dir_travel, l_area_id, r_area_id, l_postcode, r_postcode, l_numzones, r_numzones, num_ad_rng, ar_auto, ar_bus, ar_taxis, ar_carpool, ar_pedest, ar_trucks, ar_traff, ar_deliv, ar_emerveh, ar_motor, paved, private, frontage, bridge, tunnel, ramp, tollway, poiaccess, contracc, roundabout, interinter, undeftraff, ferry_type, multidigit, maxattr, spectrfig, indescrib, manoeuvre, dividerleg, inprocdata, full_geom, urban, route_type, dironsign, explicatbl, nameonrdsn, postalname, stalename, vanityname, junctionnm, exitname, scenic_rt, scenic_nm, fourwhldr, coverind, plot_road, reversible, expr_lane, carpoolrd, phys_lanes, ver_trans, pub_access, low_mblty, priorityrd, spd_lm_src, expand_inc, trans_area
INTO here_gis.streets_att
FROM here_gis.streets;
COMMENT ON TABLE here_gis.streets_att IS 'Attributes for the streets layer';
ALTER TABLE here_gis.streets_att ADD PRIMARY KEY(link_id);
SELECT link_id, geom
INTO TEMP TABLE streets_temp
FROM here_gis.streets;
DROP TABLE here_gis.streets;
SELECT link_id, geom
INTO TABLE here_gis.streets
FROM streets_temp;
ALTER TABLE here_gis.streets ADD PRIMARY KEY(link_id);
CREATE INDEX ON here_gis.streets USING gist(geom);



CREATE OR REPLACE FUNCTION here_gis.split_streets_att (revision TEXT)
RETURNS INTEGER
AS $$
DECLARE

    att_tablename TEXT;
    street_tablename TEXT;
	geom_exists BOOLEAN;
BEGIN

    att_tablename := 'streets_att_'||revision;
    street_tablename := 'streets_'||revision;
    
    EXECUTE format(' CREATE TABLE here_gis.%I AS 
                   SELECT link_id, st_name, feat_id, st_langcd, num_stnmes, st_nm_pref, st_typ_bef, st_nm_base, st_nm_suff, st_typ_aft, st_typ_att, addr_type, l_refaddr, l_nrefaddr, l_addrsch, l_addrform, r_refaddr, r_nrefaddr, r_addrsch, r_addrform, ref_in_id, nref_in_id, n_shapepnt, func_class, speed_cat, fr_spd_lim, to_spd_lim, to_lanes, from_lanes, enh_geom, lane_cat, divider, dir_travel, l_area_id, r_area_id, l_postcode, r_postcode, l_numzones, r_numzones, num_ad_rng, ar_auto, ar_bus, ar_taxis, ar_carpool, ar_pedest, ar_trucks, ar_traff, ar_deliv, ar_emerveh, ar_motor, paved, private, frontage, bridge, tunnel, ramp, tollway, poiaccess, contracc, roundabout, interinter, undeftraff, ferry_type, multidigit, maxattr, spectrfig, indescrib, manoeuvre, dividerleg, inprocdata, full_geom, urban, route_type, dironsign, explicatbl, nameonrdsn, postalname, stalename, vanityname, junctionnm, exitname, scenic_rt, scenic_nm, fourwhldr, coverind, plot_road, reversible, expr_lane, carpoolrd, phys_lanes, ver_trans, pub_access, low_mblty, priorityrd, spd_lm_src, expand_inc, trans_area
                  FROM here_gis.%I
        ; ALTER TABLE here_gis.%I OWNER TO here_admins;
        ', att_tablename, street_tablename, att_tablename);
		
    SELECT EXISTS (
 	SELECT column_name
	FROM information_schema.columns
	WHERE table_name = street_tablename AND column_name = 'geom'
    ) INTO geom_exists;
	
	IF geom_exists THEN
	    EXECUTE format('
	        COMMENT ON TABLE here_gis.%I IS ''Attributes for the streets layer'';
	        SELECT link_id, geom
	        INTO TEMP TABLE streets_temp
	        FROM here_gis.%I;
	        DROP TABLE here_gis.%I;
	        SELECT link_id, geom
	        INTO TABLE here_gis.%I
	        FROM streets_temp;
	        ALTER TABLE here_gis.%I ADD PRIMARY KEY(link_id);
	        CREATE INDEX ON here_gis.%I USING gist(geom);
	        ', att_tablename, street_tablename, street_tablename,  street_tablename,  street_tablename,  street_tablename); 
	ELSE
	EXECUTE FORMAT('
			COMMENT ON TABLE here_gis.%I IS ''Attributes for the streets layer'';
	        SELECT link_id, wkb_geometry AS geom
	        INTO TEMP TABLE streets_temp
	        FROM here_gis.%I;
	        DROP TABLE here_gis.%I;
	        SELECT link_id, geom
	        INTO TABLE here_gis.%I
	        FROM streets_temp;
	        ALTER TABLE here_gis.%I ADD PRIMARY KEY(link_id);
	        CREATE INDEX ON here_gis.%I USING gist(geom);
	        ', att_tablename, street_tablename, street_tablename,  street_tablename,  street_tablename,  street_tablename); 
END IF;		
    
    RETURN 1;
END;
$$
SECURITY DEFINER
LANGUAGE plpgsql;
ALTER FUNCTION here_gis.split_streets_att (TEXT) OWNER TO here_admins;
GRANT EXECUTE ON FUNCTION here_gis.split_streets_att (TEXT) TO here_admin_bot;
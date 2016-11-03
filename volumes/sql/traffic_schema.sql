--
-- PostgreSQL database dump
--

-- Dumped from database version 9.5.2
-- Dumped by pg_dump version 9.5.1

-- Started on 2016-11-03 18:17:23

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 20 (class 2615 OID 372132976)
-- Name: traffic; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA traffic;


--
-- TOC entry 4379 (class 0 OID 0)
-- Dependencies: 20
-- Name: SCHEMA traffic; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA traffic IS 'Tables from the Flow database';


SET search_path = traffic, pg_catalog;

SET default_with_oids = false;

--
-- TOC entry 407 (class 1259 OID 374450815)
-- Name: acc; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE acc (
    accnb character varying(10) NOT NULL,
    relaccnb character varying(10),
    accdate timestamp(0) without time zone,
    day_no character varying(1),
    acctime character varying(4),
    patarea character varying(4),
    stname1 character varying(35),
    streetype1 character varying(4),
    dir1 character varying(1),
    stname2 character varying(35),
    streetype2 character varying(4),
    dir2 character varying(1),
    stname3 character varying(35),
    streetype3 character varying(4),
    dir3 character varying(1),
    per_inv character varying(2),
    veh_inv character varying(2),
    municipal character varying(2),
    loccoord character varying(2),
    impctarea character varying(2),
    acclass character varying(2),
    accloc character varying(2),
    traffictl character varying(2),
    drivage character varying(2),
    veh_no character varying(2),
    vehtype character varying(2),
    towedveh character varying(2),
    initdir character varying(2),
    impactype character varying(2),
    imploc character varying(2),
    event1 character varying(2),
    event2 character varying(2),
    event3 character varying(2),
    per_no character varying(2),
    invtype character varying(2),
    invage character varying(2),
    injury character varying(1),
    safequip character varying(2),
    drivact character varying(2),
    drivcond character varying(2),
    pedcond character varying(2),
    pedact character varying(2),
    charge character varying(5),
    charge2 character varying(5),
    charge3 character varying(5),
    charge4 character varying(5),
    visible character varying(2),
    light character varying(2),
    rdsfcond character varying(2),
    vehimptype character varying(2),
    manoeuver character varying(2),
    entry character varying(1),
    geocode character varying(6),
    factor_err character varying(1),
    rep_type character varying(1),
    badge_no character varying(5),
    surname character varying(35),
    given character varying(50),
    str_number character varying(6),
    street character varying(35),
    type character varying(10),
    dir character varying(1),
    apt character varying(6),
    city character varying(16),
    prov character varying(4),
    postal character varying(7),
    xcoord character varying(10),
    ycoord character varying(10),
    precise_xy character varying(1),
    longitude double precision,
    latitude double precision,
    changed smallint,
    sent_unit character varying(12),
    sent_date timestamp(0) without time zone,
    status character varying(12),
    city_area character varying(12),
    user_id character varying(12),
    crc_unit character varying(12),
    comments character varying(250),
    mtp_division character varying(12),
    police_agency character varying(12),
    submit_badge_number character varying(10),
    submit_date timestamp(0) without time zone,
    birthdate timestamp(0) without time zone,
    private_property character varying(1),
    person_id bigint,
    userid character varying(50),
    ts timestamp(0) without time zone,
    road_class character varying(50),
    symbol_num bigint,
    rotation_num bigint,
    px character varying(10),
    district character varying(30),
    quadrant character varying(5),
    failtorem smallint,
    year character varying(5),
    rec_id bigint NOT NULL,
    pedtype character varying(2),
    cyclistype character varying(2),
    sidewalkcycle character varying(10),
    cycact character varying(2),
    cyccond character varying(2),
    mvaimg smallint,
    wardnum character varying(40),
    fatal_no double precision,
    description character varying(4000),
    tab_report character varying(500),
    actual_speed smallint,
    posted_speed smallint
);


--
-- TOC entry 401 (class 1259 OID 372132977)
-- Name: arterydata; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE arterydata (
    arterycode bigint NOT NULL,
    geomcode bigint,
    street1 character varying(30),
    street1type character varying(10),
    street1dir character varying(5),
    street2 character varying(30),
    street2type character varying(10),
    street2dir character varying(5),
    street3 character varying(30),
    street3type character varying(5),
    street3dir character varying(5),
    stat_code character varying(20),
    count_type character varying(10),
    location character varying(65),
    apprdir character varying(10),
    sideofint character varying(1),
    linkid character varying(50),
    seq_order character varying(7),
    geo_id bigint
);


--
-- TOC entry 410 (class 1259 OID 374450868)
-- Name: cal; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE cal (
    id bigint NOT NULL,
    count_info_id bigint,
    comment_ character varying(100),
    am_pk_hr timestamp(0) without time zone,
    pm_pk_hr timestamp(0) without time zone,
    off_pk_hr timestamp(0) without time zone,
    am_2_hr timestamp(0) without time zone,
    pm_2_hr timestamp(0) without time zone,
    am_nb_x bigint,
    am_nb_t bigint,
    am_nb_r bigint,
    am_nb_l bigint,
    am_nb_b bigint,
    am_nb_o bigint,
    am_ns_p bigint,
    am_sb_x bigint,
    am_sb_t bigint,
    am_sb_r bigint,
    am_sb_l bigint,
    am_sb_b bigint,
    am_sb_o bigint,
    am_ss_p bigint,
    am_wb_x bigint,
    am_wb_t bigint,
    am_wb_r bigint,
    am_wb_l bigint,
    am_wb_b bigint,
    am_wb_o bigint,
    am_ws_p bigint,
    am_eb_x bigint,
    am_eb_t bigint,
    am_eb_r bigint,
    am_eb_l bigint,
    am_eb_b bigint,
    am_eb_o bigint,
    am_es_p bigint,
    pm_nb_x bigint,
    pm_nb_t bigint,
    pm_nb_r bigint,
    pm_nb_l bigint,
    pm_nb_b bigint,
    pm_nb_o bigint,
    pm_ns_p bigint,
    pm_sb_x bigint,
    pm_sb_t bigint,
    pm_sb_r bigint,
    pm_sb_l bigint,
    pm_sb_b bigint,
    pm_sb_o bigint,
    pm_ss_p bigint,
    pm_wb_x bigint,
    pm_wb_t bigint,
    pm_wb_r bigint,
    pm_wb_l bigint,
    pm_wb_b bigint,
    pm_wb_o bigint,
    pm_ws_p bigint,
    pm_eb_x bigint,
    pm_eb_t bigint,
    pm_eb_r bigint,
    pm_eb_l bigint,
    pm_eb_b bigint,
    pm_eb_o bigint,
    pm_es_p bigint,
    of_nb_x bigint,
    of_nb_t bigint,
    of_nb_r bigint,
    of_nb_l bigint,
    of_nb_b bigint,
    of_nb_o bigint,
    of_ns_p bigint,
    of_sb_x bigint,
    of_sb_t bigint,
    of_sb_r bigint,
    of_sb_l bigint,
    of_sb_b bigint,
    of_sb_o bigint,
    of_ss_p bigint,
    of_wb_x bigint,
    of_wb_t bigint,
    of_wb_r bigint,
    of_wb_l bigint,
    of_wb_b bigint,
    of_wb_o bigint,
    of_ws_p bigint,
    of_eb_x bigint,
    of_eb_t bigint,
    of_eb_r bigint,
    of_eb_l bigint,
    of_eb_b bigint,
    of_eb_o bigint,
    of_es_p bigint,
    tl_nb_x bigint,
    tl_nb_t bigint,
    tl_nb_r bigint,
    tl_nb_l bigint,
    tl_nb_b bigint,
    tl_nb_o bigint,
    tl_ns_p bigint,
    tl_sb_x bigint,
    tl_sb_t bigint,
    tl_sb_r bigint,
    tl_sb_l bigint,
    tl_sb_b bigint,
    tl_sb_o bigint,
    tl_ss_p bigint,
    tl_wb_x bigint,
    tl_wb_t bigint,
    tl_wb_r bigint,
    tl_wb_l bigint,
    tl_wb_b bigint,
    tl_wb_o bigint,
    tl_ws_p bigint,
    tl_eb_x bigint,
    tl_eb_t bigint,
    tl_eb_r bigint,
    tl_eb_l bigint,
    tl_eb_b bigint,
    tl_eb_o bigint,
    tl_es_p bigint
);


--
-- TOC entry 409 (class 1259 OID 374450865)
-- Name: category; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE category (
    category_id bigint NOT NULL,
    category_name character varying(20)
);


--
-- TOC entry 408 (class 1259 OID 374450821)
-- Name: cnt_det; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE cnt_det (
    id bigint NOT NULL,
    count_info_id bigint,
    count bigint DEFAULT 0,
    timecount timestamp(0) without time zone,
    speed_class integer
);


--
-- TOC entry 402 (class 1259 OID 372132984)
-- Name: cnt_occ; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE cnt_occ (
    id bigint NOT NULL,
    count_info_id bigint,
    occupancy bigint DEFAULT 0,
    timecount date
);


--
-- TOC entry 404 (class 1259 OID 372133004)
-- Name: cnt_spd; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE cnt_spd (
    id bigint NOT NULL,
    count_info_id bigint,
    speed bigint DEFAULT 0,
    timecount date
);


--
-- TOC entry 415 (class 1259 OID 374450891)
-- Name: cnt_speed_vol; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE cnt_speed_vol (
    id bigint,
    count_info_id bigint,
    volume smallint,
    speed numeric(6,3),
    timecount timestamp(0) without time zone,
    channel_id character(36)
);


--
-- TOC entry 414 (class 1259 OID 374450883)
-- Name: cnt_sum; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE cnt_sum (
    id bigint,
    count_info_id bigint DEFAULT 0,
    beg_datetime timestamp(0) without time zone,
    end_datetime timestamp(0) without time zone,
    count_int character varying(4),
    dataconfig character varying(1),
    am_pk_hr timestamp(0) without time zone,
    am_pk_vol bigint DEFAULT 0,
    pm_pk_hr timestamp(0) without time zone,
    pm_pk_vol bigint DEFAULT 0,
    off_pk_hr timestamp(0) without time zone,
    off_pk_vol bigint DEFAULT 0,
    tot_count bigint DEFAULT 0
);


--
-- TOC entry 405 (class 1259 OID 372133012)
-- Name: countinfo; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE countinfo (
    count_info_id bigint NOT NULL,
    arterycode bigint DEFAULT 0,
    count_date date,
    day_no bigint DEFAULT 0,
    comment_ character varying(250),
    file_name character varying(100),
    source1 character varying(50),
    source2 character varying(50),
    load_date timestamp(0) without time zone,
    speed_info_id bigint DEFAULT 0,
    category_id bigint
);


--
-- TOC entry 514 (class 1259 OID 461426120)
-- Name: countinfomics; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE countinfomics (
    count_info_id bigint NOT NULL,
    arterycode bigint,
    count_type character varying(1),
    count_date timestamp(0) without time zone,
    day_no bigint,
    comment_ character varying(250),
    file_name character varying(100),
    load_date timestamp(0) without time zone,
    transfer_rec smallint,
    category_id bigint
);


--
-- TOC entry 416 (class 1259 OID 374450901)
-- Name: daily_adj_factors; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE daily_adj_factors (
    road_class double precision,
    year_num double precision,
    month_num double precision,
    month_name character varying(10),
    day_num double precision,
    day_name character varying(10),
    adjustment_factor double precision,
    sample_size double precision,
    count_ double precision
);


--
-- TOC entry 403 (class 1259 OID 372132998)
-- Name: det; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE det (
    id bigint NOT NULL,
    count_info_id bigint,
    count_time timestamp without time zone,
    n_cars_r bigint,
    n_cars_t bigint,
    n_cars_l bigint,
    s_cars_r bigint,
    s_cars_t bigint,
    s_cars_l bigint,
    e_cars_r bigint,
    e_cars_t bigint,
    e_cars_l bigint,
    w_cars_r bigint,
    w_cars_t bigint,
    w_cars_l bigint,
    n_truck_r bigint,
    n_truck_t bigint,
    n_truck_l bigint,
    s_truck_r bigint,
    s_truck_t bigint,
    s_truck_l bigint,
    e_truck_r bigint,
    e_truck_t bigint,
    e_truck_l bigint,
    w_truck_r bigint,
    w_truck_t bigint,
    w_truck_l bigint,
    n_bus_r bigint,
    n_bus_t bigint,
    n_bus_l bigint,
    s_bus_r bigint,
    s_bus_t bigint,
    s_bus_l bigint,
    e_bus_r bigint,
    e_bus_t bigint,
    e_bus_l bigint,
    w_bus_r bigint,
    w_bus_t bigint,
    w_bus_l bigint,
    n_peds bigint,
    s_peds bigint,
    e_peds bigint,
    w_peds bigint,
    n_bike bigint,
    s_bike bigint,
    e_bike bigint,
    w_bike bigint,
    n_other bigint,
    s_other bigint,
    e_other bigint,
    w_other bigint
);


--
-- TOC entry 417 (class 1259 OID 374450904)
-- Name: eight_hour_exp_factors; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE eight_hour_exp_factors (
    road_class double precision,
    year_num double precision,
    month_num double precision,
    month_name character varying(10),
    day_num double precision,
    day_name character varying(10),
    hour_type character(1),
    adjustment_factor double precision,
    sample_size double precision,
    count_ double precision
);


--
-- TOC entry 420 (class 1259 OID 374450913)
-- Name: hourly_adj_factors; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE hourly_adj_factors (
    road_class double precision,
    year_num double precision,
    month_num double precision,
    month_name character varying(10),
    day_num double precision,
    day_name character varying(10),
    hour_num double precision,
    adjustment_factor double precision,
    sample_size double precision,
    count_ double precision
);


--
-- TOC entry 413 (class 1259 OID 374450880)
-- Name: lit; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE lit (
    id bigint,
    linkid character varying(50),
    arcid bigint
);


--
-- TOC entry 406 (class 1259 OID 374450805)
-- Name: load_cnt_det; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE load_cnt_det (
    load_cnt_det_id character(36),
    load_count_info_id character(36),
    file_load_id character(36),
    tot_count bigint DEFAULT 0,
    count_date date,
    speed_class bigint,
    opposite_dir character(1)
);


--
-- TOC entry 419 (class 1259 OID 374450910)
-- Name: monthly_adj_factors; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE monthly_adj_factors (
    road_class double precision,
    year_num double precision,
    month_num double precision,
    month_name character varying(10),
    adjustment_factor double precision,
    sample_size double precision,
    count_ double precision
);


--
-- TOC entry 418 (class 1259 OID 374450907)
-- Name: perm_stn_haf; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE perm_stn_haf (
    arterycode bigint,
    road_class bigint,
    year_num double precision,
    month_num double precision,
    day_num double precision,
    hour_num double precision,
    haf_value double precision,
    no_of_days double precision
);


--
-- TOC entry 412 (class 1259 OID 374450877)
-- Name: report_category; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE report_category (
    report_category_id bigint,
    repnumber bigint,
    category_id bigint
);


--
-- TOC entry 411 (class 1259 OID 374450871)
-- Name: rescu_countinfo; Type: TABLE; Schema: traffic; Owner: -
--

CREATE TABLE rescu_countinfo (
    count_info_id bigint,
    arterycode bigint DEFAULT 0,
    count_date timestamp(0) without time zone,
    day_no bigint DEFAULT 0,
    comment_ character varying(250),
    file_name character varying(100),
    source1 character varying(50),
    source2 character varying(50),
    load_date timestamp(0) without time zone,
    speed_info_id bigint DEFAULT 0,
    category_id bigint
);


--
-- TOC entry 4225 (class 2606 OID 374450810)
-- Name: arterydata_pkey; Type: CONSTRAINT; Schema: traffic; Owner: -
--

ALTER TABLE ONLY arterydata
    ADD CONSTRAINT arterydata_pkey PRIMARY KEY (arterycode);


--
-- TOC entry 4239 (class 2606 OID 461426129)
-- Name: cal_pkey; Type: CONSTRAINT; Schema: traffic; Owner: -
--

ALTER TABLE ONLY cal
    ADD CONSTRAINT cal_pkey PRIMARY KEY (id);


--
-- TOC entry 4237 (class 2606 OID 461426131)
-- Name: category_pkey; Type: CONSTRAINT; Schema: traffic; Owner: -
--

ALTER TABLE ONLY category
    ADD CONSTRAINT category_pkey PRIMARY KEY (category_id);


--
-- TOC entry 4235 (class 2606 OID 461426133)
-- Name: cnt_det_pkey; Type: CONSTRAINT; Schema: traffic; Owner: -
--

ALTER TABLE ONLY cnt_det
    ADD CONSTRAINT cnt_det_pkey PRIMARY KEY (id);


--
-- TOC entry 4227 (class 2606 OID 374450812)
-- Name: cnt_occ_pkey; Type: CONSTRAINT; Schema: traffic; Owner: -
--

ALTER TABLE ONLY cnt_occ
    ADD CONSTRAINT cnt_occ_pkey PRIMARY KEY (id);


--
-- TOC entry 4231 (class 2606 OID 374450814)
-- Name: cnt_spd_pkey; Type: CONSTRAINT; Schema: traffic; Owner: -
--

ALTER TABLE ONLY cnt_spd
    ADD CONSTRAINT cnt_spd_pkey PRIMARY KEY (id);


--
-- TOC entry 4233 (class 2606 OID 372133033)
-- Name: countinfo_pkey; Type: CONSTRAINT; Schema: traffic; Owner: -
--

ALTER TABLE ONLY countinfo
    ADD CONSTRAINT countinfo_pkey PRIMARY KEY (count_info_id);


--
-- TOC entry 4241 (class 2606 OID 461426127)
-- Name: countinfomics_pkey; Type: CONSTRAINT; Schema: traffic; Owner: -
--

ALTER TABLE ONLY countinfomics
    ADD CONSTRAINT countinfomics_pkey PRIMARY KEY (count_info_id);


--
-- TOC entry 4229 (class 2606 OID 374450900)
-- Name: det_pkey; Type: CONSTRAINT; Schema: traffic; Owner: -
--

ALTER TABLE ONLY det
    ADD CONSTRAINT det_pkey PRIMARY KEY (id);


-- Completed on 2016-11-03 18:17:24

--
-- PostgreSQL database dump complete
--


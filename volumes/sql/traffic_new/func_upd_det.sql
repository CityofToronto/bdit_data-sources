-- FUNCTION: traffic.update_det()

-- DROP FUNCTION IF EXISTS traffic.update_det();

CREATE OR REPLACE FUNCTION traffic.update_det(
	)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

    insert into traffic.det (
	select * from "TRAFFIC_NEW"."DET"
	except
	select * from traffic.det)
	on conflict (id) 
	DO UPDATE 
		SET count_info_id = EXCLUDED.count_info_id,
			count_time = EXCLUDED.count_time,
			n_cars_r = EXCLUDED.n_cars_r,
			n_cars_t = EXCLUDED.n_cars_t,
			n_cars_l = EXCLUDED.n_cars_l,
			s_cars_r = EXCLUDED.s_cars_r,
			s_cars_t = EXCLUDED.s_cars_t,
			s_cars_l = EXCLUDED.s_cars_l,
			e_cars_r = EXCLUDED.e_cars_r,
			e_cars_t = EXCLUDED.e_cars_t,
			e_cars_l = EXCLUDED.e_cars_l,
			w_cars_r = EXCLUDED.w_cars_r,
			w_cars_t = EXCLUDED.w_cars_t,
			w_cars_l = EXCLUDED.w_cars_l,
			n_truck_r = EXCLUDED.n_truck_r,
			n_truck_t = EXCLUDED.n_truck_t,
			n_truck_l = EXCLUDED.n_truck_l,
			s_truck_r = EXCLUDED.s_truck_r,
			s_truck_t = EXCLUDED.s_truck_t,
			s_truck_l = EXCLUDED.s_truck_l,
			e_truck_r = EXCLUDED.e_truck_r,
			e_truck_t = EXCLUDED.e_truck_t,
			e_truck_l = EXCLUDED.e_truck_l,
			w_truck_r = EXCLUDED.w_truck_r,
			w_truck_t = EXCLUDED.w_truck_t,
			w_truck_l = EXCLUDED.w_truck_l,
			n_bus_r = EXCLUDED.n_bus_r,
			n_bus_t = EXCLUDED.n_bus_t,
			n_bus_l = EXCLUDED.n_bus_l,
			s_bus_r = EXCLUDED.s_bus_r,
			s_bus_t = EXCLUDED.s_bus_t,
			s_bus_l = EXCLUDED.s_bus_l,
			e_bus_r = EXCLUDED.e_bus_r,
			e_bus_t = EXCLUDED.e_bus_t,
			e_bus_l = EXCLUDED.e_bus_l,
			w_bus_r = EXCLUDED.w_bus_r,
			w_bus_t = EXCLUDED.w_bus_t,
			w_bus_l = EXCLUDED.w_bus_l,
			n_peds = EXCLUDED.n_peds,
			s_peds = EXCLUDED.s_peds,
			e_peds = EXCLUDED.e_peds,
			w_peds = EXCLUDED.w_peds,
			n_bike = EXCLUDED.n_bike,
			s_bike = EXCLUDED.s_bike,
			e_bike = EXCLUDED.e_bike,
			w_bike = EXCLUDED.w_bike,
			n_other = EXCLUDED.n_other,
			s_other = EXCLUDED.s_other,
			e_other = EXCLUDED.e_other,
			w_other = EXCLUDED.w_other;

with delrec as (
	select * from traffic.det 
	except 
	select * from "TRAFFIC_NEW"."DET")

Delete from traffic.det where id in (SELECT id from delrec);

$BODY$;

ALTER FUNCTION traffic.update_det()
    OWNER TO traffic_admins;

GRANT EXECUTE ON FUNCTION traffic.update_det() TO scannon;

GRANT EXECUTE ON FUNCTION traffic.update_det() TO traffic_bot;

REVOKE ALL ON FUNCTION traffic.update_det() FROM PUBLIC;

update traffic_bylaw_clean 
set time_day = regexp_replace(time_day, 'Monday to Friday', 'Mon. to Fri.');

update traffic_by_clean
set time_day = regexp_replace(time_day, 'Public', 'public');

update traffic_by_clean
set time_day = regexp_replace(time_day, 'Holidays', 'holidays');

update traffic_bylaw_clean
set time_day = regexp_replace(time_day, 'holiday', 'holidays);

update traffic_bylaw_clean
set time_day = regexp_replace(time_day, 'holidayss', 'holidays);

update traffic_bylaw_clean
set time_day = regexp_replace(time_day, 'ppublic', 'public');

update traffic_bylaw_clean
set time_day = regexp_replace(time_day, '[\f\ \t\v\b\r\n\u00a0]', ' ', 'g');

update traffic_bylaw_clean
set time_day = regexp_replace(time_day, '  ', ' ', 'g');

update traffic_bylaw_clean 
set time_day = regexp_replace(time_day, 'p.m,|p.m..,', 'p.m.,');

update traffic_bylaw_clean
set time_day= regexp_replace(time_day, '.Mon', ', Mon') 
where time_day not like '%,_Mon%' and time_day like '%Mon%';

update traffic_bylaw_clean
set time_day = regexp_replace(time_day, '.except public holidays', ', except public holidays')
where time_day not like '%,_except public holidays%' and time_day like '%except public holiday%';

update traffic_bylaw_clean
set time_day = 'Anytime' 
where time_day = 'All times';

update traffic_bylaw_clean set time_day = 'Anytime' where time_day = 'Anytime during T.T.C. labour disruption';

update traffic_bylaw_clean set time_day = 'Anytime' where time_day = 'Anytime (TTC vehicles excepted)' ;
 


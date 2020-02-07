-- The following sql does *not* extract information out of all rows (only around 98% of them)

select time_day as og_timeday, legend_id, 
		  		 --public holidays as binary
				 --public holidays as binary
				 case when time_day ~~* '%except_public_holidays%' or time_day ~~* '%except_%_public_holidays' 
							then 1 else 0 end as holiday,
				 --get dow as integer[]
	  		 	 case when (time_day ~~* '%Mon._to_Fri%' or time_day ~~*  '%Monday_to_Friday%') and not time_day ~~* '%except%Mon._to_Fri%' 
				 			then array[1,2,3,4,5] 
		          	  when time_day ~~* '%Mon._to_Sat.%'and not time_day ~~* '%except%Mon._to_Sat%' 
					  		then array[1,2,3,4,5,6]
		  			  when time_day similar to '%next_following_day'
		  					then array[1,2,3,4,5,6,7]
					  when time_day similar to '\d*:\d\d__.m._to_\d*:\d\d__.m.'
					  		then array[1,2,3,4,5,6,7]
			     	  when time_day ~~* 'Anytime' 
					  		then array[1,2,3,4,5,6,7] end as dow, 
				 --see if maxium period is in the str
 			     case when time_day ~~* '%for_a_maximum_period%' 
				 			then 1 else 0 end as max_period,
				 --get time_day with the following case			
				 case when  time_day similar to '\d*:\d\d__.m._to_\d*:\d\d__.m.'
					  		then array[timerange((array(select(to_timestamp(unnest(split_ref(time_day::text)), 'HH:MI a.m.')::time without time zone)))[1],
						 	(array(select(to_timestamp(unnest(split_ref(time_day::text)), 'HH:MI a.m.')::time without time zone)))[2])]
				 	  when (regexp_split_to_array(time_day, ','))[1] similar to '\d*:\d\d__.m._to_\d*:\d\d__.m.' 
 							and ((regexp_split_to_array(time_day, ','))[2] similar to '%\d*:\d\d__.m._to_\d*:\d\d__.m.'
 							and (regexp_split_to_array(time_day, ','))[2] not similar to '%except%')
 					  		then array[
									timerange((array(select(to_timestamp(unnest(split_ref((regexp_split_to_array(time_day, ','))[1])), 'HH:MI a.m.')::time without time zone)))[1],
											(array(select(to_timestamp(unnest(split_ref((regexp_split_to_array(time_day, ','))[1])), 'HH:MI a.m.')::time without time zone)))[2]),								
									timerange((array(select(to_timestamp(unnest(split_ref((regexp_split_to_array(time_day, ','))[2])), 'HH:MI a.m.')::time without time zone)))[1],
											(array(select(to_timestamp(unnest(split_ref((regexp_split_to_array(time_day, ','))[2])), 'HH:MI a.m.')::time without time zone)))[2])]
					  when (regexp_split_to_array(time_day, ','))[1] similar to '\d*:\d\d__.m._to_\d*:\d\d__.m.' 
							and ((regexp_split_to_array(time_day, ','))[2] not similar to '%\d*:\d\d__.m._to_\d*:\d\d__.m.'
							and (regexp_split_to_array(time_day, ','))[2] not similar to '%except%')
							then array[
								  timerange(
								 (array(select(to_timestamp(unnest(split_ref((regexp_split_to_array(time_day, ','))[1])), 'HH:MI a.m.')::time without time zone)))[1],
								 (array(select(to_timestamp(unnest(split_ref((regexp_split_to_array(time_day, ','))[1])), 'HH:MI a.m.')::time without time zone)))[2])]							 
					  when time_day similar to '%next_following_day'
							then array[
									timerange(
										'00:00:00', 
										(array(select(to_timestamp(unnest(split_ref((regexp_split_to_array(time_day, ','))[1])), 'HH:MI a.m.')::time without time zone)))[2]), 
									timerange(
										(array(select(to_timestamp(unnest(split_ref((regexp_split_to_array(time_day, ','))[1])), 'HH:MI a.m.')::time without time zone)))[1],
										'23:59:59')]
																	 
					 when time_day = 'Anytime' 
				  			then array['[00:00:00,23:59:59)'::timerange]
						    else null end as time_day_array  
from natalie.traffic_bylaw_clean  
														  

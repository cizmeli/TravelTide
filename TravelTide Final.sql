
-- aggregated, ready for segmentation
with
cancelled_trips as -- this block is used to identify the cancelled trips in next step
(
  select 
  	s.trip_id,s.cancellation
  from sessions s
  where s.trip_id is not null
  and s.cancellation
),
user_trips as -- in this block some initial calculations are made per user_id and trip_id level to merge with reservastion details in next step
(
  select
   s.trip_id
  ,s.user_id
  ,u.gender
  ,case u.home_country 
  	when 'usa' then 0
  	when 'canada' then 1
   end as home_country
  ,u.home_city
  ,u.home_airport
  ,u.home_airport_lon
  ,u.home_airport_lat
  ,(now()::date - u.sign_up_date::date)/30::int AS months_since_sign_up
  ,u.has_children
  ,u.married as is_married
  ,case when u.has_children and u.married then true else false end as is_family
  ,ROUND(EXTRACT(YEAR FROM AGE(NOW(), u.birthdate)) + 
 		EXTRACT(MONTH FROM AGE(NOW(), u.birthdate)) / 12 , 2) AS age_float
-- for all calculations I ignored trips that are cancelled  		
  ,coalesce(ct.cancellation,false) as cancellation
  ,(case when not coalesce(ct.cancellation,false) and (s.flight_discount or s.hotel_discount) then 1 else 0 end) as discounted_trips
  ,(case when not coalesce(ct.cancellation,false) and s.hotel_discount then 1 else 0 end) as hotel_discounted_trips
  ,sum(case when not coalesce(ct.cancellation,false) and s.flight_discount then 1 else 0 end) as flight_discounted_trips
  ,min(case when not coalesce(ct.cancellation,false) then s.session_start end) as reservation_date
  ,max(coalesce(case when not coalesce(ct.cancellation,false) then s.flight_discount_amount end, 0)) as flight_discount_amount
  ,max(coalesce(case when not coalesce(ct.cancellation,false) then s.hotel_discount_amount end, 0)) as hotel_discount_amount
  ,count(s.session_id) as number_of_sessions_per_trip
  ,sum(s.page_clicks) as number_of_page_click_per_trip
  ,round(sum(EXTRACT(EPOCH FROM (s.session_end - s.session_start)) / 60), 2) AS session_duration_per_trip_in_minutes
  from sessions s
    left join users u on s.user_id = u.user_id
  	left join cancelled_trips ct on ct.trip_id = s.trip_id
  where 1=1 
  and s.session_start >= '2023-01-05'
  and s.session_id != '590420-814ccf31a57445a889176d909b2c2057' -- this session is excluded because of unusual number of clicks count
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)
,
reservations as
(
  select
  coalesce(f.trip_id,h.trip_id) as trip_id
  ,h.trip_id as hotel_trip_id
  ,h.hotel_name
  ,SPLIT_PART(h.hotel_name, ' - ', 1) AS hotel_group
  ,SPLIT_PART(h.hotel_name, ' - ', 2) AS hotel_city
  ,h.nights as nights_orj
  ,case 
  	when h.nights<=0 and f.return_time is not null and f.return_time::date - h.check_in_time::date!=0 
  		then f.return_time::date - h.check_in_time::date
  	when h.nights<=0 and f.return_time is not null and f.return_time::date - h.check_in_time::date=0
  		then 1
  	when h.nights<=0 and f.return_time is null and h.check_out_time::date<=h.check_in_time::date
  		then 1
  	when h.nights<=0 and f.return_time is null and h.check_out_time::date>h.check_in_time::date
  		then h.check_out_time::date - h.check_in_time::date
   else h.nights end as nights -- here I fixed the bookings with <=0 nights values by using primeraly using check in-out, then return time  
  ,h.rooms
  ,h.check_in_time
  ,h.check_out_time
  ,h.hotel_per_room_usd
  ,f.trip_id as flight_trip_id
  ,f.origin_airport
  ,f.destination
  ,f.destination_airport
  ,f.seats
  ,f.return_flight_booked
  ,f.departure_time
  ,f.return_time
  ,f.checked_bags
  ,f.trip_airline
  ,f.destination_airport_lat
  ,f.destination_airport_lon
  ,f.base_fare_usd
  from flights f 
    full join hotels h on f.trip_id=h.trip_id
  where 1=1  
)
,
general_aggregations as
(
  select
   ut.user_id
  ,case ut.gender when 'F' then 0
                  when 'M' then 1
                  when 'O' then 2
   end as gender
  ,ut.home_country
  ,ut.home_city
  ,ut.months_since_sign_up
  ,case when ut.has_children then 1 else 0 end as has_children 
  ,case when ut.is_married then 1 else 0 end as is_married
  ,case when ut.is_family then 1 else 0 end as is_family
  ,ut.age_float::int as age
  ,case when round(ut.age_float) between 18 and 25 then '18-25'
        when round(ut.age_float) between 26 and 30 then '26-30'
        when round(ut.age_float) between 31 and 40 then '31-40'
        when round(ut.age_float) between 41 and 50 then '41-50'
        when round(ut.age_float) > 50 then '50+' end as age_group
  ,coalesce(avg(coalesce(r.departure_time,r.check_in_time)::date-ut.reservation_date::date), 0) as avg_days_from_res_to_trip
  ,coalesce(sum(ut.number_of_sessions_per_trip), 0) as total_number_of_sessions 
  ,coalesce(sum(ut.number_of_page_click_per_trip), 0) as total_number_of_page_clicks 
  ,coalesce(sum(ut.session_duration_per_trip_in_minutes), 0) as total_session_duration_in_minutes
  ,coalesce(count(case when not ut.cancellation then ut.trip_id end), 0) as total_trips
  ,coalesce(sum(ut.discounted_trips), 0) as total_discounted_trips 
  ,coalesce(sum(ut.hotel_discounted_trips), 0) as total_hotel_discounted_trips
  ,coalesce(sum(ut.flight_discounted_trips), 0) as total_flight_discounted_trips
  ,coalesce(count(case when ut.cancellation then ut.trip_id end), 0) as total_cancelled_trips
  ,coalesce(sum(
    case when not ut.cancellation and r.flight_trip_id is not null and not r.return_flight_booked then 1
         when not ut.cancellation and r.flight_trip_id is not null and r.return_flight_booked then 2
    end
   ), 0) as total_flights 
  ,coalesce(sum(
    case when not ut.cancellation and r.flight_trip_id is not null and r.return_flight_booked then 1
    end
   ), 0) as return_flights
  ,coalesce(sum(
    case when not ut.cancellation and r.hotel_trip_id is not null then 1
    end
   ), 0) as total_hotel_res
   ,coalesce(sum(
    case when not ut.cancellation then r.base_fare_usd
    end
   ), 0) as total_paid_for_flights_before_disc
   ,round(coalesce(avg(
    case when not ut.cancellation then r.base_fare_usd
    end
   ), 0), 2) as avg_paid_for_flights_before_disc
  ,coalesce(sum(
    case when not ut.cancellation then r.base_fare_usd*(1-coalesce(ut.flight_discount_amount,0))
    end
   ), 0) as total_paid_for_flights_after_disc
   ,round(coalesce(avg(
    case when not ut.cancellation then r.base_fare_usd*(1-coalesce(ut.flight_discount_amount,0))
    end
   ), 0), 2) as avg_paid_for_flights_after_disc
   ,coalesce(sum(
    case when not ut.cancellation then r.base_fare_usd*coalesce(ut.flight_discount_amount,0)
    end
   ), 0) as total_disc_for_flights
   ,round(coalesce(avg(
    case when not ut.cancellation then r.base_fare_usd*coalesce(ut.flight_discount_amount,0)
    end
   ), 0), 2) as avg_disc_for_flight 
  ,round(coalesce(avg(
    case when not ut.cancellation then coalesce(ut.flight_discount_amount,0)
    end
   ), 0), 2) as avg_disc_perc_for_flight
  ,round(coalesce(avg(
    case when not ut.cancellation then coalesce(ut.hotel_discount_amount,0)
    end
   ), 0), 2) as avg_disc_perc_for_hotel   
  ,coalesce(sum(
    case when not ut.cancellation then r.hotel_per_room_usd*r.rooms*r.nights
    end
   ), 0) as total_paid_for_hotels_before_disc
   ,round(coalesce(avg(
    case when not ut.cancellation then r.hotel_per_room_usd*r.rooms*r.nights
    end
   ), 0), 2) as avg_paid_for_hotels_before_disc
   ,coalesce(sum(
    case when not ut.cancellation then r.hotel_per_room_usd*r.rooms*r.nights*(1-coalesce(ut.hotel_discount_amount,0))
    else 0
    end
   ), 0) as total_paid_for_hotels_after_disc
   ,round(coalesce(avg(
    case when not ut.cancellation then r.hotel_per_room_usd*r.rooms*r.nights*(1-coalesce(ut.hotel_discount_amount,0))
    end
   ), 0), 2) as avg_paid_for_hotels_after_disc
   ,coalesce(sum(
    case when not ut.cancellation then r.hotel_per_room_usd*r.rooms*r.nights*coalesce(ut.hotel_discount_amount,0)
    end
   ), 0) as total_disc_for_hotels
   ,round(coalesce(avg(
    case when not ut.cancellation then r.hotel_per_room_usd*r.rooms*r.nights*coalesce(ut.hotel_discount_amount,0)
    end
   ), 0), 2) as avg_disc_for_hotels
  ,round(coalesce(avg(
    case when not ut.cancellation then r.rooms 
    end
   ), 0), 2) as avg_num_of_rooms_per_hotel_res
  ,round(coalesce(avg(
    case when not ut.cancellation then r.nights 
    end
   ), 0), 2) as avg_num_of_nights_per_hotel_res
   ,coalesce(sum(
    case when not ut.cancellation then r.nights 
    end
   ), 0) as tot_num_of_nights_for_hotel_res
  ,coalesce(count(distinct
    case when not ut.cancellation then r.hotel_name 
    end
   ), 0) as num_of_diff_hotels_stayed
  ,coalesce(count(distinct
    case when not ut.cancellation then r.hotel_group 
    end
   ), 0) as num_of_diff_hotel_groups_stayed
  ,coalesce(count(distinct
    case when not ut.cancellation then r.hotel_city 
    end
   ), 0) as num_of_diff_hotel_cities_stayed
  ,coalesce(count(distinct
    case when not ut.cancellation then r.destination 
    end
   ), 0) as num_of_diff_flight_dest
  ,coalesce(count(distinct
    case when not ut.cancellation then r.trip_airline 
    end
   ), 0) as num_of_diff_airlines
  ,coalesce(count(distinct
    case when not ut.cancellation then coalesce(r.origin_airport,ut.home_city)||coalesce(r.destination,r.hotel_city)
    end
   ), 0) as num_of_diff_routes
  ,round(coalesce(avg(
    case when not ut.cancellation then r.seats
    end
   ), 0), 2) as avg_num_of_seats_booked_per_flight
  ,coalesce(sum(
    case when not ut.cancellation then r.seats 
    end
   ), 0) as tot_num_of_seats_booked_for_flights
  ,coalesce(sum(
    case when not ut.cancellation then r.checked_bags 
    end
   ), 0) as tot_number_of_checked_bags_for_flights
  ,round(coalesce(avg(
    case when not ut.cancellation then r.checked_bags
    end
   ), 0), 2) as avg_of_checked_bags_for_flights
  ,coalesce(sum(
    case when r.flight_trip_id is not null and r.return_flight_booked and not ut.cancellation then
          6371 * acos(
          cos(radians(r.destination_airport_lat)) * cos(radians(ut.home_airport_lat)) *
          cos(radians(ut.home_airport_lon) - radians(r.destination_airport_lon)) +
          sin(radians(r.destination_airport_lat)) * sin(radians(ut.home_airport_lat))
          ) *2
         when r.flight_trip_id is not null and not r.return_flight_booked and not ut.cancellation then
          6371 * acos(
          cos(radians(r.destination_airport_lat)) * cos(radians(ut.home_airport_lat)) *
          cos(radians(ut.home_airport_lon) - radians(r.destination_airport_lon)) +
          sin(radians(r.destination_airport_lat)) * sin(radians(ut.home_airport_lat))
          )
    end) ,0) AS tot_flight_distance_km         
  ,coalesce(sum(
    case when not ut.cancellation and r.return_flight_booked then haversine_distance(home_airport_lat, home_airport_lon, destination_airport_lat, destination_airport_lon)*2 
         when not ut.cancellation and not r.return_flight_booked then haversine_distance(home_airport_lat, home_airport_lon, destination_airport_lat, destination_airport_lon)
    end) ,0) as tot_flight_dist_km
  ,coalesce(avg(
    case when not ut.cancellation and r.return_flight_booked then haversine_distance(home_airport_lat, home_airport_lon, destination_airport_lat, destination_airport_lon)*2 
         when not ut.cancellation and not r.return_flight_booked then haversine_distance(home_airport_lat, home_airport_lon, destination_airport_lat, destination_airport_lon)
    end) ,0) as avg_flight_dist_km 
  from user_trips ut 
      left join reservations r using(trip_id)
  where 1=1
  and ut.age_float >= 18 -- I realised that for some trips users were under aged adn that is why i excluded them
  group by 1,2,3,4,5,6,7,8,9,10
  having sum(ut.number_of_sessions_per_trip)>7 -- this is to exclude the users who does not have at least 8 sessions
)
select 
 *
,case when total_number_of_sessions/nullif(total_trips,0) <= 2 then 'Rare Booker' 
	  when total_number_of_sessions/nullif(total_trips,0) <= 5 then 'Occasional Booker' 
	  when total_number_of_sessions/nullif(total_trips,0) <= 8 then 'Frequent Booker' 
	  when coalesce(total_trips,0)=0 then 'Never Booker'
	  else 'Super Booker' end as booking_rate_per_session_segmentation
,case when avg_paid_for_flights_after_disc+avg_paid_for_hotels_after_disc <= 1000 then 'Low Budget' 
	  when avg_paid_for_flights_after_disc+avg_paid_for_hotels_after_disc <= 2000 then 'Moderate Budget' 
	  when avg_paid_for_flights_after_disc+avg_paid_for_hotels_after_disc <= 3000 then 'High Budget' 
	  else 'Luxury Budget' end as budget_segmentation  
,case when total_flights <= 4 then 'Rare Flyer' 
	  when total_flights <= 9 then 'Regular Flyer' 
	  when total_flights <= 12 then 'Frequent Flyer' 
	  else 'Bird' end as flyer_segmentation  	  
,coalesce((total_paid_for_flights_after_disc+total_paid_for_hotels_after_disc)/nullif(total_trips,0),0) as customer_value
,coalesce(total_paid_for_flights_after_disc/nullif(tot_flight_dist_km,0),0) as ADS_per_km
,coalesce(total_flight_discounted_trips/nullif(total_trips,0),0) as discount_flight_proportion
,coalesce(coalesce(total_paid_for_flights_after_disc/nullif(tot_flight_dist_km,0),0)*
	coalesce(total_flight_discounted_trips/nullif(total_trips,0),0)*
	avg_disc_perc_for_flight,0) as bargain_hunter_index
,coalesce(total_session_duration_in_minutes/total_number_of_sessions,0) as avg_session_dur_min	
from general_aggregations
where 1=1
;






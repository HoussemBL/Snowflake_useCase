CREATE DATABASE flights_DB;



--create internal stage
create or replace stage flight_stage;



--copy data from local to 
--using SNowSQL CLI:  snowsql -a pybzryw-pf71715  -u HoussemBL
--then enter password

LIST @flight_stage ;




-- Create a file format.CREATE OR REPLACE FILE FORMAT myformat TYPE = 'csv' FIELD_DELIMITER = ',' skip_header = 1;





--create table flights
CREATE or replace TABLE flights (
    FL_DATE Date,
	OP_CARRIER varchar(20),
	OP_CARRIER_FL Number,
	origin varchar(20),
    dest varchar(20),
	CRS_DEP_TIME integer,
	DEP_TIME integer,
	DEP_DELAY integer,
	TAXI_OUT  integer,
	WHEELS_OFF  number,
	WHEELS_ON  number,
	TAXI_IN  integer,
	CRS_ARR_TIME  number,
	ARR_TIME  number,
	ARR_DELAY integer,
	CANCELLED  integer,
	CANCELLATION_CODE String,
	DIVERTED integer,
	CRS_ELAPSED_TIME integer,
	ACTUAL_ELAPSED_TIME integer,
	AIR_TIME integer,
	DISTANCE number,
	CARRIER_DELAY integer,
	WEATHER_DELAY integer,
	NAS_DELAY integer,
	SECURITY_DELAY integer,
	LATE_AIRCRAFT_DELAY integer,
	unamed text);




--load data to table flights
COPY INTO flights from @flight_stage  file_format = myformat FORCE=True ;


--check if flights table is not empty
select count(*) from flights;







-- small transformation of date
create or replace view v_flights as select FL_DATE,Year(FL_DATE) as FL_year,Month(FL_DATE) as FL_month,Day(FL_DATE) as FL_day,Quarter(FL_DATE) as FL_quarter,dayofweek(FL_DATE) as FL_dayofweek, OP_CARRIER, OP_CARRIER_FL, ORIGIN, DEST, CRS_DEP_TIME, DEP_TIME, DEP_DELAY, TAXI_OUT, WHEELS_OFF, WHEELS_ON, TAXI_IN, CRS_ARR_TIME, ARR_TIME, ARR_DELAY, CANCELLED, CANCELLATION_CODE, DIVERTED, CRS_ELAPSED_TIME, ACTUAL_ELAPSED_TIME, AIR_TIME, DISTANCE, CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY from flights;



--departure delay view
create or replace view v_delay_flights as (select count(*) as nb_delayed_flights,avg(dep_delay) as avg_delay_in_mintes, max(dep_delay) as max_delay_in_mintes , sum(dep_delay) as sum_delay_in_minutes,
                                           
                                                  avg(carrier_delay) as avg_carrier_delay_in_mintes, max(carrier_delay) as max_carrier_delay_in_mintes , sum(carrier_delay) as sum_carrier_delay_in_minutes,                                                
avg(weather_delay) as avg_weather_delay_in_mintes, max(weather_delay) as max_weather_delay_in_mintes , sum(weather_delay) as sum_weather_delay_in_minutes,
        
                                                  OP_CARRIER,FL_year,FL_DATE,FL_month,FL_day,FL_quarter,FL_dayofweek
                                       from v_flights where  dep_delay>0
                                       group by OP_CARRIER,FL_year,FL_DATE,FL_month,FL_day,FL_quarter,FL_dayofweek);
                                       
                                       


--cancellation view
create or replace view v_canceled_flights as (select count(*) as num_cancelation,OP_CARRIER,cast(FL_year as varchar(10)),FL_DATE,FL_month,FL_day,FL_quarter,FL_dayofweek,CANCELLATION_CODE
                                       from v_flights where cancelled=1
                                       group by OP_CARRIER,FL_year,FL_DATE,FL_month,FL_day,FL_quarter,FL_dayofweek,CANCELLATION_CODE);








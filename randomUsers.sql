create database random_users_db;



create or replace stage snowpipe_users_stage url='s3://snowflake-bucket-source/'
CREDENTIALS=(AWS_KEY_ID='xxxxx' AWS_SECRET_KEY='xxxxxx')
;



list @snowpipe_users_stage;


CREATE or replace temporary TABLE users_json (json_data_raw VARIANT);



CREATE OR REPLACE PIPE users_pipe 
auto_ingest = true
AS
COPY INTO users_json from @snowpipe_users_stage file_format = (type=json);


SHOW PIPES;


//check pipe status
select SYSTEM$PIPE_STATUS('users_pipe');


--check if data is loaded to table users_json
select count(*) from users_json ;



------create stream
create stream mystream on table users_json;

select * from mystream;

select system$stream_has_data('MYSTREAM');



--create a task: whenever a new data is inserted to table users_jsons, flatten the data and save it in table users_info
create TASK mytask 
warehouse=COMPUTE_WH 
SCHEDULE= '1 minute'
WHEN system$stream_has_data('MYSTREAM')
as INSERT INTO  users_info(gender,nationality,firstname,lastname,city,state,country) 
select 
value:gender::String,
value:nat::String ,
value:name:first::String ,
value:name:last::String ,
value:location:city::String,
value:location:state::String,
value:location:country::String 
from 
mystream,
lateral flatten (input => json_data_raw:results)
where 		METADATA$ACTION=	'INSERT'	
				;

--task management
show tasks;  
alter Task    mytask resume;            
alter Task    mytask suspend; 
 
 
--check if data isloaded in table users_info
select count(*) from users_info;



--create a view that will be visualized in PowerBI
create or replace view v_users_info as 
(select count(*) as num_users,state,city,gender,nationality,country from users_info group by state,city,gender,nationality,country order by num_users desc);



select * from v_users_info limit 100;

select count(*) from v_users_info;

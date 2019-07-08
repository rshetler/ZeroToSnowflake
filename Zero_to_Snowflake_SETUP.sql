use role sysadmin;  
  
--Create a database that we'll use  
-- to create the necessary tables & views  
create database citibike_data;  
  
-- Use the newly created database  
use citibike_data.public;  
  
-- Create a table to store the TRIPS data.  
-- This table will contain information about  
-- each trip the Citibike riders took over the  
-- last few years  
create or replace table TRIPS (  
    TRIPDURATION NUMBER(38,0),  
    STARTTIME TIMESTAMP_NTZ(9),  
    STOPTIME TIMESTAMP_NTZ(9),  
    START_STATION_ID NUMBER(38,0),  
    START_STATION_NAME VARCHAR(16777216),  
    START_STATION_LATITUDE FLOAT,  
    START_STATION_LONGITUDE FLOAT,  
    END_STATION_ID NUMBER(38,0),  
    END_STATION_NAME VARCHAR(16777216),  
    END_STATION_LATITUDE FLOAT,  
    END_STATION_LONGITUDE FLOAT,  
    BIKEID NUMBER(38,0),  
    NAME VARCHAR(16777216),  
    USERTYPE VARCHAR(16777216),  
    BIRTH_YEAR NUMBER(38,0),  
    GENDER NUMBER(38,0)  
);  
  
-- Create a table that will be used to  
-- store weather observations  
create or replace table weather (v variant, t timestamp);  
  
-- Create a reference to the S3 location where the  
-- demo data resides.  
create or replace stage citibike_s3_stage URL = 's3://sfc-citibike-demo/'  
CREDENTIALS = (AWS_KEY_ID = '****************'   
               AWS_SECRET_KEY = '****************************');  
  
-- Create a compute cluster to load the data.  This cluster  
-- will be 8 nodes, suspend automatically after 1 minute of inactivity and resume instantly when a new query or job comes in  
create or replace warehouse load_wh   
    warehouse_size=xlarge   
    auto_suspend = 60  
    auto_resume = true  
    initially_suspended=true ;  
  
-- For this session, use the warehouse we just created  
-- to load the data  
use warehouse load_wh;  
  
-- Copy the bike trips data into the table you created  
-- This load will load 246 files in parallel into this table  
copy into trips from @citibike_s3_stage/trips/ ;  
  
-- Copy the weather data which is in JSON format.  
-- This will load 184 files in parallel into this table  
copy into weather   
from (select $1, convert_timezone('UTC', 'US/Eastern', $1:time::timestamp_ntz)   
        from @citibike_s3_stage/weather/) file_format = (type = json) ;  
          
-- Suspend the cluster you just created   
alter warehouse load_wh suspend;  
  
  
-- create a view that joins the information about a specific trip to the weather info  
create or replace  view trip_weather_vw as  
select *  
from trips  
  left outer join  
  ( select t as observation_time  
    ,v:city.id::int as city_id  
    ,v:city.name::string as city_name  
    ,v:city.country::string as country  
    ,v:city.coord.lat::float as city_lat  
    ,v:city.coord.lon::float as city_lon  
    ,v:clouds.all::int as clouds  
    ,(v:main.temp::float)-273.15 as temp_avg  
    ,(v:main.temp_min::float)-273.15 as temp_min  
    ,(v:main.temp_max::float)-273.15 as temp_max  
    ,v:weather[0].id::int as weather_id  
    ,v:weather[0].main::string as weather  
    ,v:weather[0].description::string as weather_desc  
    ,v:weather[0].icon::string as weather_icon  
    ,v:wind.deg::float as wind_dir  
    ,v:wind.speed::float as wind_speed  
   from weather  
   where city_id = 5128638)  
  on date_trunc(HOUR, starttime) = date_trunc(HOUR, observation_time);  
    
-- Now create a warehouse that the Tableau   
-- workbook will use.    
create or replace warehouse tableau_wh  
    warehouse_size=medium   
    auto_suspend = 5  
    auto_resume = true  
    initially_suspended=true ;  
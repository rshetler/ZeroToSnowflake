----------------------------------------------------------------------------------
-- Zero to Snowflake : Hands on lab
----------------------------------------------------------------------------------

--Ensure we are using the role 'SYSADMIN' for demo purposes, and the citibike_data database we created
use role sysadmin;  
use citibike_data.public; 

-- Create a compute cluster to analyze our data. This cluster will be 4 nodes, suspend automatically after 1 
-- minute of inactivity and resume instantly when a new query or job comes in  

create or replace warehouse query_wh   
    warehouse_size=large   
    auto_suspend = 60  
    auto_resume = true  
    initially_suspended=true; 

use warehouse query_wh;

----------------------------------------------------------------------------------
-- Handle semi-structured data like relational data with a view
----------------------------------------------------------------------------------

--Query weather info related to Rochester
select   
    t as observation_time
    ,v:city.name::string as city_name  
    ,v:city.country::string as country  
    ,(v:main.temp::float)-214.84 as temp_avg  
    ,(v:main.temp_min::float)-214.84 as temp_min  
    ,(v:main.temp_max::float)-214.84 as temp_max    
    ,v:weather[0].main::string as weather  
    ,v:weather[0].description::string as weather_desc  
    ,v:wind.deg::float as wind_dir  
    ,v:wind.speed::float as wind_speed  
   from weather  
   where city_name like 'Rochester'
   order by temp_avg desc;

--Query occurances of a weather type
select 
    v:weather[0].main::string as WEATHER, 
    count(*) as WEATHER_COUNT
    from weather
    where v:city.name::string like 'Rochester'
    group by weather
    order by WEATHER_COUNT desc;

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

--Query the view like a relational table 
select tripduration, weather_desc from trip_weather_vw where weather_desc = 'sky is clear' order by tripduration desc;
select tripduration, weather_desc from trip_weather_vw where weather_desc = 'light snow' order by tripduration desc;
select tripduration, weather_desc from trip_weather_vw where weather_desc = 'heavy snow' order by tripduration desc;

--Utiize window functions 
select
        AVG(TRIPDURATION) / 60 AS AVG_TRIP_DURATION_MINS,
        2019 - BIRTH_YEAR  AS AGE,
        COUNT(*) AS TOTAL
    from trip_weather_vw
    where BIRTH_YEAR is not null and BIRTH_YEAR > 1930
    group by age
    order by total desc; 

----------------------------------------------------------------------------------
-- Time travel 
----------------------------------------------------------------------------------

--Test UNDROP
select count(*) from trips;
drop table trips;
select count(*) from trips;
undrop table trips;
select count(*) from trips;


--Test TRUNCATE and restore
select count(*) from trips;

delete from trips where starttime > '2014-07-25 24:00:00.000';

select
    (select count(*) from trips AT(OFFSET => -60*3)) before_delete,
    (select count(*) from trips) after_delete;


--Create table from time travel data
create table restored_trips clone trips before (statement => '*********');

select count(*) from restored_trips;
show tables;

--Swap restored table with main trips table
alter table restored_trips swap with trips;
drop table restored_trips;
select count(*) from trips;
show tables;

--Set data retention period per TABLE
show tables;
ALTER TABLE trips SET data_retention_time_in_days = 30; 
show tables;

--Set data retention period for entire schema
alter database citibike_data set data_retention_time_in_days = 30;
show tables;


----------------------------------------------------------------------------------
-- Scale virtual warehouse 
----------------------------------------------------------------------------------
--Try a query with a large table scan 
select * from trips where tripduration > 1000;

--Change the size of the warehouse to service the large query
alter warehouse query_wh set warehouse_size="XXLarge";

select * from trips where tripduration > 1000;

--Resize warehouse back down after query is finished
alter warehouse query_wh set warehouse_size="SMALL";

----------------------------------------------------------------------------------
-- Results Cache
----------------------------------------------------------------------------------
--Run long running query
alter warehouse query_wh set warehouse_size="XXLarge";
select * from trips where tripduration > 2000;

--Rerun same query - view history
select * from trips where tripduration > 2000;

----------------------------------------------------------------------------------
-- External Tables
----------------------------------------------------------------------------------

--Create stage for external table 
-- ***TO DO*** Update the fields below with the key information you downloaded earlier
create or replace stage external_table_stage URL = 's3://sfc-external-table-demo/external_table'  
CREDENTIALS = (AWS_KEY_ID = '*****************'   
               AWS_SECRET_KEY = '*******************************'); 

--Create external table connected to external stage
create or replace external table ext_sales_data (
L_ORDERKEY NUMBER(38,0) as (value:c1::number(38,0)) ,
L_PRODUCTKEY NUMBER(38,0) as (value:c2::number(38,0)),
L_SUPPKEY NUMBER(38,0) as (value:c3::number(38,0)),
L_LINENUMBER NUMBER(38,0) as (value:c4::number(38,0)),
L_QUANTITY NUMBER(12,2) as (value:c5::number(12,2)),
L_EXTENDEDPRICE NUMBER(12,2) as (value:c6::number(12,2)),
L_DISCOUNT NUMBER(12,2) as (value:c7::number(12,2)),
L_TAX NUMBER(12,2) as (value:c8::number(12,2)),
L_RETURNFLAG VARCHAR(1) as (value:c9::varchar(1)),
L_LINESTATUS VARCHAR(1) as (value:c10::varchar(1)),
L_ORDERDATE1 DATE as (value:c11::date),
L_ORDERDATE DATE as to_date(substr(metadata$filename, 16,10), 'YYYY/MM/DD'),
L_COMMITDATE DATE as (value:c12::date),
L_RECEIPTDATE DATE as (value:c13::date),
L_SHIPINSTRUCT VARCHAR(25) as (value:c14::varchar(25)),
L_SHIPMODE VARCHAR(10) as (value:c15::varchar(10)),
L_COMMENT VARCHAR(44) as (value:c16::varchar(44))
)
 partition by (l_orderdate)
 location = @external_table_stage/
Auto_refresh = false;

--Refresh external table to build meta data
alter external table if exists ext_sales_data refresh;

--Show table
describe table ext_sales_data;

--Query the external table with standard SQL statements
select sum(l_quantity) as  SUM_QUANTITY,
    avg(l_discount) as AVG_DISCOUNT
    from ext_sales_data
    where l_orderdate = '1996-11-30'
    group by  l_suppkey;
    
--Add new rows to the external table
alter table ext_sales_data add column L_REGION INTEGER as to_number(substr(metadata$filename, 32,1));
alter table ext_sales_data add column L_DIVISION integer as to_number(substr(metadata$filename, 34,1));

--Refresh external table after new rows are added
alter external table if exists ext_sales_data refresh;

--Run query with further pruning
select sum(l_quantity) as  SUM_QUANTITY,
    avg(l_discount) as AVG_DISCOUNT
    from ext_sales_data
    where l_orderdate = '1996-11-30' and (l_region = 4) and (l_division = 7)
    group by  l_suppkey;


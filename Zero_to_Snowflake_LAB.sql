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
-- Handle semi-structured data like relational data with view
----------------------------------------------------------------------------------
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

select tripduration, weather_desc from trip_weather_vw where weather_desc = 'sky is clear' order by tripduration desc;
select tripduration, weather_desc from trip_weather_vw where weather_desc = 'light snow' order by tripduration desc;
select tripduration, weather_desc from trip_weather_vw where weather_desc = 'heavy snow' order by tripduration desc;


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
-- Time travel can also drive CDC...
----------------------------------------------------------------------------------
-- change a few records
update trips set start_station_name = 'TEST'
where start_station_id = 3252;

-- get the query ID of the update statement, hold the result in a SQL variable
set query_id =
  (select query_id
   from table(information_schema.query_history_by_session(result_limit => 100))
   where query_text like 'update%' order by start_time desc limit 1);

-- show the changed records
select * from trips                                    -- after the update
minus
select * from trips before (statement => $query_id);   -- before the update

----------------------------------------------------------------------------------
-- Clustering
----------------------------------------------------------------------------------
--Enable clustering on column "starttime"
show tables;
alter table trips cluster by (starttime);
show tables;

--Suspend auto clustering
alter table trips suspend recluster;
show tables;

----------------------------------------------------------------------------------
-- Stored Proceedures 
----------------------------------------------------------------------------------
--Create table to store Float values
CREATE TABLE stproc_test_table1 (num_col1 numeric(14,7));

--Define stored proceedure to evaluate and insert float value into the table
create or replace procedure stproc1(FLOAT_PARAM1 FLOAT)
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    if (FLOAT_PARAM1 > 4) {
        var sql_command = 
        "INSERT INTO stproc_test_table1 (num_col1) VALUES (" + FLOAT_PARAM1 + ")";
        try {
            snowflake.execute (
                {sqlText: sql_command}
                );
            return "Succeeded.";   // Return a success/error indicator.
            }
        catch (err)  {
            return "Failed: " + err;   // Return a success/error indicator.
            }
    }
    else {
    return "Too Small";
    }
    $$
    ;

--Call stored proceedure with small value
call stproc1(3.14::FLOAT);

--Call stored proceedure with larger value
call stproc1(6.234::FLOAT);

--Verify values were inserted
select * from stproc_test_table1;

--Define stored proceedure to return number of rows in a given table. Dynamically generate the SQL 
create or replace procedure get_row_count(table_name VARCHAR)
  returns float not null
  language javascript
  as
  $$
  var row_count = 0;
  // Dynamically compose the SQL statement to execute.
  var sql_command = "select count(*) from " + TABLE_NAME;
  // Run the statement.
  var stmt = snowflake.createStatement(
         {
         sqlText: sql_command
         }
      );
  var res = stmt.execute();
  // Get back the row count. Specifically, ...
  // ... get the first (and in this case only) row from the result set ...
  res.next();
  // ... and then get the returned value, which in this case is the number of
  // rows in the table.
  row_count = res.getColumnValue(1);
  return row_count;
  $$
  ;
  
--Call row count stored proceedure to return row count
call get_row_count('trips');

--Evaluate row count returned
select count(*) from trips;

--Stored proceedure to create roles - returns false if role already exists
create or replace procedure create_role(ROLENAME String)
    returns boolean
    language javascript
    strict
    as
    $$
        var create_sql_command = 
        "CREATE ROLE " + ROLENAME + "";
        try {
            snowflake.execute (
                {sqlText: create_sql_command});             
            return true;   // Return a success/error indicator.
            }
        catch (err)  {
            return false;   // Return a success/error indicator.
            }
    $$
    ;

call create_role('NEWROLETEST');

Show roles;

--Stored proceedure used to grant all privileges on given database - error if role or database doesn't exist
create or replace procedure grant_all_to_role(ROLENAME String, DBNAME String)
    returns string
    language javascript
    strict
    as
    $$
        var create_sql_command = 
        "GRANT all on database " + DBNAME + " to role " + ROLENAME + "";
        try {
            snowflake.execute (
                {sqlText: create_sql_command});             
            return "Success";   // Return a success/error indicator.
            }
        catch (err)  {
            return "Failed: " + err;   // Return a success/error indicator.
            }
    $$
    ;

--Call to grant all to given role on database
call grant_all_to_role('NEWROLETEST', 'CITIBIKE_DATA');

--Try with errors
call grant_all_to_role('NEWROLETEST', 'CITIBIKE');
call grant_all_to_role('NEWROLE', 'CITIBIKE_DATA');


--Display grants on the CITIBIKE_DATA database
show grants on database CITIBIKE_DATA;


----------------------------------------------------------------------------------
-- Automate SQL using Tasks
----------------------------------------------------------------------------------

--Create table to store subset of trip data
create or replace table SUBSET_TABLE (  
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

--Create a table to store only trip duration and calculated age
create or replace table TRIPDURATION_BY_AGE (  
    TRIPDURATION NUMBER(38,0),  
    AGE NUMBER(38,0) 
); 


--Insert a given set of data into the subset_table
insert into SUBSET_TABLE (
    select * from trips where starttime between '2014-07-28 24:00:00.000' and '2014-07-29 24:00:00.000' 
  );


--Create new role called 'taskadmin' which can be granted to users who are allowed to execute tasks in the account
use role securityadmin;
create role taskadmin;
-- set the active role to ACCOUNTADMIN before granting the EXECUTE TASK privilege to the new role
use role accountadmin;
--Grant execute task priviledge to the new taskadmin role
grant execute task on account to role taskadmin;
-- set the active role to SECURITYADMIN to show that this role can grant a role to another role
use role securityadmin;
--Grant the new taskadmin role to the current SYSADMIN role we are using
grant role taskadmin to role SYSADMIN;
--Switch back to SYSADMIN role 
use role sysadmin;

-- create task to insert rows from the activity_raw stream into the transformation table
    
create task insert_into_tripdurationbyage
  warehouse = query_wh
  schedule = '1 minute' as
  insert into TRIPDURATION_BY_AGE (
    select
        TRIPDURATION AS TRIP_DURATION,
        2019 - BIRTH_YEAR  AS AGE
    from subset_table
    where BIRTH_YEAR is not null 
);

--View our task
show tasks;

--Allow the task to start
alter task insert_into_tripdurationbyage resume;

show tasks;

--WAIT 1 MINUTE - Query to view data inserted into table
select count(*) from TRIPDURATION_BY_AGE;
select * from TRIPDURATION_BY_AGE order by age desc;

--Set task to suspended
alter task insert_into_tripdurationbyage suspend;
show tasks;


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

----------------------------------------------------------------------------------
-- Streaming data ingestion with Snowpipe
----------------------------------------------------------------------------------

--Create a stage to an external S3 bucket to be used with snowpipe
create or replace stage snowpipe_stage URL = 's3://sfc-snowpipe-bucket/'  
CREDENTIALS = (AWS_KEY_ID = '*******************'   
               AWS_SECRET_KEY = '**********************************'); 

--List contents of stage
list @snowpipe_stage;

--Create a table to load data into from snowpipe
--Create table to hold relational data
CREATE or REPLACE TABLE citibike_data.public.snowpipetable ("C1" STRING, "C2" STRING, "C3" STRING, "C4" STRING, "C5" STRING, "C6" STRING, "C7" STRING);

--Create the snowpipe definition, using a COPY INTO statement from the external stage
create pipe snowpipe_demo auto_ingest=true as
    copy into citibike_data.public.snowpipetable
    from @snowpipe_stage
    file_format = (type = 'CSV');

--Show pipe definition to view notification channel
show pipes;

--Refresh Pipe
alter pipe snowpipe_demo refresh;

select count(*) from citibike_data.public.snowpipetable;

select * from citibike_data.public.snowpipetable;

truncate table citibike_data.public.snowpipetable;
 
/* Azure Steps:
Open Azure CLI or Shell
az group create --name snowpipe --location eastus
az provider register --namespace Microsoft.EventGrid
az provider show --namespace Microsoft.EventGrid --query "registrationState" -- CHECK UNTIL "Registered" status
az storage account create --resource-group snowpipe --name sfcsnowpipe --sku Standard_LRS --location eastus --kind BlobStorage --access-tier Hot
az storage account create --resource-group snowpipe --name sfcstoragequeue --sku Standard_LRS --location eastus --kind StorageV2

az storage queue create --name sfcstoragequeue --account-name sfcsnowpipe
broken 

*/

----------------------------------------------------------------------------------
-- Working with Internal Stages using PUT
----------------------------------------------------------------------------------

--Create table to hold relational data
CREATE TABLE citibike_data.public.testtable ("C1" STRING, "C2" STRING, "C3" STRING, "C4" STRING, "C5" STRING, "C6" STRING, "C7" STRING);

--Create table to hold semi-structured data
CREATE TABLE citibike_data.public.testjson ("CATEGORY" VARIANT);


--Create internal Snowflake Managed stage
CREATE STAGE citibike_data.public.internalstage;

--Use PUT to stage files via Snowsql
/*
use role sysadmin;
use warehouse query_wh;
use citibike_data.public;

Single File
PUT file:///Users/rshetler/Documents/Snowpipe/CSV/result00001.csv @internalstage;
All CSVs
PUT file:///Users/rshetler/Documents/Snowpipe/CSV/* @internalstage;
All JSON in subfolder
PUT file:///Users/rshetler/Documents/Snowpipe/JSON/* @internalstage/json;
*/

--Copy into only Column 1 from Column 2 of a file;
copy into testtable(c1)
  from (select t.$2 from @internalstage/result00001.csv.gz t);
  
select * from testtable;

--Copy files that match the *.csv.gz naming convention 
copy into TESTTABLE from @internalstage file_format = (type = 'CSV') pattern='.*/.*/.*[.]csv[.]gz';

select * from testtable;

--Create JSON file format 
CREATE FILE FORMAT "CITIBIKE_DATA"."PUBLIC".JSON TYPE = 'JSON' COMPRESSION = 'AUTO' ENABLE_OCTAL = FALSE 
ALLOW_DUPLICATE = FALSE STRIP_OUTER_ARRAY = FALSE STRIP_NULL_VALUES = FALSE IGNORE_UTF8_ERRORS = FALSE;

--Copy JSON files into TESTJSON table from the subfolder json on internal stage
copy into TESTJSON from @internalstage/json/ file_format='JSON';

select * from TESTJSON;
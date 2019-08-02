----------------------------------------------------------------------------------
-- Zero to Snowflake Advanced Concepts 
----------------------------------------------------------------------------------
--Setup worksheet context
use role accountadmin; 
use citibike_data.public;
use warehouse query_wh;



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
    returns string
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
            return err;   // Return a success/error indicator.
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
call grant_all_to_role('NEWROLTEST', 'CITIBIE');
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
use role ACCOUNTADMIN;

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
CREATE or REPLACE TABLE citibike_data.public.snowpipetable (  
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
Alter variable names in place of the <text> below and paste the code between the lines into the Azure CLI: 
--------------------------------------------------------------------------------------------------------------------------------------------------------------
resourceGroup=<snowpipe>
location=<eastus>
dataStorageAccount=<sfcsnowpipe>
queueStorageAccount=<sfcstoragequeueaccount>
queueName=<sfcstoragequeue>
eventGridName=<eventgridsnowpipe>

az group create --name $resourceGroup --location $location
az provider register --namespace Microsoft.EventGrid
sleep 30
az provider show --namespace Microsoft.EventGrid --query "registrationState" 

az storage account create --resource-group $resourceGroup --name $dataStorageAccount --sku Standard_LRS --location eastus --kind BlobStorage --access-tier Hot
az storage account create --resource-group $resourceGroup --name $queueStorageAccount --sku Standard_LRS --location eastus --kind StorageV2
az storage queue create --name $queueName --account-name $queueStorageAccount

export storageid=$(az storage account show --name $dataStorageAccount --resource-group $resourceGroup --query id --output tsv)
export queuestorageid=$(az storage account show --name $queueStorageAccount --resource-group $resourceGroup --query id --output tsv)
export queueid="$queuestorageid/queueservices/default/queues/$queueName"

az extension add --name eventgrid
az eventgrid event-subscription create --source-resource-id $storageid --name $eventGridName --endpoint-type storagequeue --endpoint $queueid
--------------------------------------------------------------------------------------------------------------------------------------------------------------

Inside the Azure portal: 

Navigate to the Queue Storage Account created - make note of the Queue URL 
EX. https://sfcstoragequeueaccount.queue.core.windows.net/sfcstoragequeue

Navigate to Azure Active Directory -> Properties - make note of the tenant ID
EX. 1*****f3-eb**-4*ef-9**2-ba22*****da

From your AZ snowflake account:

create or replace notification integration azsnowpipe
  enabled = true
  type = queue
  notification_provider = azure_storage_queue
  azure_storage_queue_primary_uri = '<QUEUE STORAGE URL>'
  azure_tenant_id = '<AZURE TENANT ID>'; 

describe notification integration azsnowpipe;
-- Click the "AZURE CONSENT URL", copy it, paste into a browser, login and select "Accept"

create or replace stage az_snowpipe_stage
  url = 'azure://<AZURE BLOB LOCATION>/'
  credentials = (azure_sas_token='****************************');

Prior to creating the actual pipe follow instructions for "Granting Snowflake Access to the Storage Queue" here:
https://docs.snowflake.net/manuals/user-guide/data-load-snowpipe-auto-azure.html#granting-snowflake-access-to-the-storage-queue

Afterwards go back into your snowflake and complete the following: 

  create or replace pipe az_pipe
  auto_ingest = true
  integration = 'AZSNOWPIPE'
  as
  copy into snowpipetable
  from @demo_db.public.az_snowpipe_stage
  file_format = (type = 'CSV');

  Check status of pipe:
  select SYSTEM$PIPE_STATUS( 'az_pipe' );
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
PUT file:///<FILE LOCATION>/result00001.csv @internalstage;
All CSVs
PUT file:///<FILE LOCATION>/CSV/* @internalstage;
All JSON in subfolder
PUT file:///U<FILE LOCATION>/JSON/* @internalstage/json;
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
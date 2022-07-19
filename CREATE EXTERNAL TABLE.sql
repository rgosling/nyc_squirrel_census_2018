-- ////////////////////////////////////////////////////////////////////////////
-- //  
-- //  Synapse Analytics Toolkit: Learn about creating External Tables
-- //
-- //  Description: 
-- //  -----------------------------------
-- //  Example of how to create an EXTERNAL TABLE in Synapse Analytics.
-- //  The data came from the 2018 Squirrel Census in Central Park, NYC
-- //  https://data.cityofnewyork.us/Environment/2018-Central-Park-Squirrel-Census-Squirrel-Data/vfnx-vebw
-- // 
-- //  Uses csv file that's included in the zip file
-- //  You need to push this file onto the Azure Data Lake storage with    
-- //  Azure Storage Explorer or something similar.
-- //   
-- //  Modification history:
-- //  Rev#  Date        Author     Description
-- //   ----- --------   ---------  -------------------------------------------
-- //  1.0   2022-May-05 R Gosling  Original Release 
-- ////////////////////////////////////////////////////////////////////////////
--========================================================================================================
--- Master DB Key
--========================================================================================================
--DROP MASTER KEY ENCRYPTION BY PASSWORD
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'XmXv2fmsBfZzc6^*&t2m^8n^HH3m7E'; 
GO
--========================================================================================================
-- Database scoped crendential
--========================================================================================================
--DROP DATABASE SCOPED CREDENTIAL msi_cred 
CREATE DATABASE SCOPED CREDENTIAL msi_cred WITH IDENTITY = 'Managed Service Identity';
GO
--========================================================================================================
-- External Data Source
--========================================================================================================
--DROP EXTERNAL DATA SOURCE ext_datasource_with_abfss 
CREATE EXTERNAL DATA SOURCE ext_datasource_with_abfss
WITH (
LOCATION = 'abfss://bronze@argonadls01.dfs.core.windows.net/',
CREDENTIAL = msi_cred); 
GO
--========================================================================================================
--  External file formats inform the system on how to handle the incoming data
--========================================================================================================
-- CSV - Skip the first row (has header)
--DROP EXTERNAL FILE FORMAT csv_skip1
CREATE EXTERNAL FILE FORMAT csv_skip1 WITH
(
  FORMAT_TYPE = DELIMITEDTEXT,
  FORMAT_OPTIONS
  (
   FIELD_TERMINATOR = ','
  ,STRING_DELIMITER = '0x22' -- Double quote hex
  ,FIRST_ROW =2
  )
)
GO
-- Skip the first row. Also use a Pipe symbol for the delimiter
--DROP EXTERNAL FILE FORMAT csv_skip1_pipe
CREATE EXTERNAL FILE FORMAT csv_skip1_pipe WITH
(
  FORMAT_TYPE = DELIMITEDTEXT,
  FORMAT_OPTIONS
  (
   FIELD_TERMINATOR = ','
  ,STRING_DELIMITER = '|' -- Pipe Symbol hex
  ,FIRST_ROW = 2
  )
)
GO
--DROP EXTERNAL FILE FORMAT parquet_fmt
CREATE EXTERNAL FILE FORMAT parquet_fmt WITH 
(
    FORMAT_TYPE = PARQUET
   ,DATA_COMPRESSION = N'org.apache.hadoop.io.compress.SnappyCodec'
)
GO
--
-- Creating a schema to use for the external tables
-- CREATE SCHEMA stg AUTHORIZATION dbo
-- GO
--
--========================================================================================================
-- Create the External Table
--========================================================================================================
IF EXISTS(SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'stg' AND name like 'squirrel_census_csv')  
   DROP EXTERNAL TABLE dbo.squirrel_census_csv; 
  --
CREATE EXTERNAL TABLE dbo.squirrel_census_csv
(
     x                         varchar(20)
    ,y                         varchar(20)
    ,squirrelid                varchar(20)
    ,hectare                   varchar(20)
    ,shift                     varchar(2)
    ,[date]                    varchar(30)
    ,hectare_squirrel_number   varchar(120)
    ,age                       varchar(20)
    ,primaryfurcolor           varchar(50)
    ,highligtfurcolor          varchar(50)
    ,prihighlightcolor         varchar(50)
    ,colornotes                varchar(200)
    ,location                  varchar(200)
    ,abovegroundmeasure        varchar(20)
    ,specloc                   varchar(250)
    ,running                   varchar(5)
    ,chasing                   varchar(5)
    ,climbing                  varchar(5)
    ,eating                    varchar(5)
    ,foraging                  varchar(5)
    ,other                     varchar(200)
    ,kuks                      varchar(5)
    ,quaas                     varchar(5)
    ,moans                     varchar(5)
    ,tailflags                 varchar(5)
    ,tailtwitches              varchar(5)
    ,approaches                varchar(5)
    ,indifferent               varchar(5)
    ,runsfrom                  varchar(5)
    ,otherinteractions         varchar(200)
    ,lat_lon                   varchar(50)
)
WITH
(
  LOCATION = 'NYCSquirrelCensus2018/', -- Top of the file structure for the table
  DATA_SOURCE = ext_datasource_with_abfss,
  FILE_FORMAT = csv_skip1
)
GO
--
SELECT TOP 500 * FROM squirrel_census_csv

--

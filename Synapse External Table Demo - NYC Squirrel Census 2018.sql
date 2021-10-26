-- ////////////////////////////////////////////////////////////////////////////
-- //
-- //  Description: 
-- //  -----------------------------------
-- //  Synapse Analytics External Table Demo using the NYC 2018 Central Park  
-- //  Squirrel Census. 
-- //
-- //  Data: 
-- //  https://data.cityofnewyork.us/Environment/2018-Central-Park-Squirrel-Census-Squirrel-Data/vfnx-vebw
-- //  
-- //  Modification history:
-- //  Rev#  Date         Author     Description
-- //   ----- --------    ---------  --------------------------------------------
-- //   1.0   2021-Oct-25 R Gosling  Original Release 
-- ////////////////////////////////////////////////////////////////////////////
--

--========================================================================================================
-- First you'll need a Master Key to encrypt things if you don't already have one. 
-- Use any password you like, I never use re-use these for security reasons and suggest you don't either.
--========================================================================================================
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'YOUR MASTER KEY HERE; -- Master Key
GO
--========================================================================================================
-- Now you need to create a database scoped crendential. Before that, you'll need a Shared Access Signature (SAS)
-- These can be created in your Azure Portal, Azure Storage Explorer and a myriad of scripting solutions
--========================================================================================================
CREATE DATABASE SCOPED CREDENTIAL argonadls -- Credential
WITH IDENTITY='SHARED ACCESS SIGNATURE',  
SECRET = '<YOUR SAS Key HERE>'
--
--========================================================================================================
-- Let's create an external Data Source that points just to the top-level storage
--========================================================================================================
CREATE EXTERNAL DATA SOURCE argon_adls  -- Data Source
WITH
(    LOCATION         = 'https://argonstore.dfs.core.windows.net/'
     , CREDENTIAL = argonadls

)
GO
--
--========================================================================================================
--  File formats describe how to handle the file we're going to work with. Is it Comma Separated Value (CSV)
--  Parquet or some other format? This is where you can make it easy to reuse the definitions
--========================================================================================================
-- The one I'll actually use
CREATE EXTERNAL FILE FORMAT argon_skip1_csv WITH
(
	FORMAT_TYPE = DELIMITEDTEXT,
	FORMAT_OPTIONS
	(
	 FIELD_TERMINATOR = ','
	,STRING_DELIMITER = '0x22' -- Double quote hex
	,FIRST_ROW = 2
	)
)
GO
--Here are some from one of the articles I referrenced
CREATE EXTERNAL FILE FORMAT QuotedCsvWithHeaderFormat
WITH (  
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS ( FIELD_TERMINATOR = ',', STRING_DELIMITER = '"', FIRST_ROW = 2   )
)
GO
CREATE EXTERNAL FILE FORMAT ParquetFormat WITH (  FORMAT_TYPE = PARQUET )
GO
CREATE EXTERNAL FILE FORMAT DeltaLakeFormat WITH (  FORMAT_TYPE = DELTA )
GO
--========================================================================================================
--  Let's create the external table. Here, I'm just guessing my worst case length wise (550). 
--  and see if there are any errors
--========================================================================================================
IF EXISTS(SELECT 1 FROM SYS.EXTERNAL_TABLES T WHERE t.name= 'squirrel_census')
    DROP EXTERNAL TABLE dbo.squirrel_census
go
CREATE EXTERNAL TABLE dbo.squirrel_census
(
	 lon                             varchar(550)
	,lat                             varchar(550)
	,squirrel_id                     varchar(550)
	,hectare                         varchar(550)
	,shift                           varchar(550)
	,sighting_date                   varchar(550)
	,hectare_squirrel_number         varchar(550)
	,age                             varchar(550)
	,primary_fur_color               varchar(550)
	,highlight_fur_color             varchar(550)
	,comb_primary_highlight_color    varchar(550)
	,color_notes                     varchar(550)
	,sighting_loc                    varchar(550)
	,above_ground_sighter_meas       varchar(550)
	,specific_location               varchar(550)
	,running                         varchar(550)
	,chasing                         varchar(550)
	,climbing                        varchar(550)
	,eating                          varchar(550)
	,foraging                        varchar(550)
	,other_activities                varchar(550)
	,kuks                            varchar(550)
	,quaas                           varchar(550)
	,moans                           varchar(550)
	,tail_flags                      varchar(550)
	,tail_twitches                   varchar(550)
	,approaches                      varchar(550)
	,indifferent                     varchar(550)
	,runs_from                       varchar(550)
	,other_Interactions              varchar(550)
	,lon_lat                         varchar(550)

)
WITH
(
	LOCATION = '/demo/nyc_squirrel_census_2018/2018_Central_Park_Squirrel_Census_-_Squirrel_Data.csv',
	DATA_SOURCE = argon_adls,
	FILE_FORMAT = argon_skip1_csv
)
GO
--========================================================================================================
-- Now try to select a few rows to see if all is well. If it isn't you'll need to fix whatever errors you get
--========================================================================================================
SELECT TOP 500 * FROM dbo.squirrel_census;
--
--========================================================================================================
-- But now, we have a table that could be better. Getting the right data types worked out, choosing a better
-- format than CSV (Parquet!!) are just a couple of things we can do here. I won't do EVERYTHING possible, because
-- I have a demo after this with Power BI and it uses Power Query to clean up the date format just to illustrate
-- data can and is modified everywhere along the path.
--
-- Let's use CREATE TABLE AS SELECT (CETAS) to restate the data into its proper data types. Of course, we can 
-- weaponize T-SQL to do a myriad of changes here (such a powerful language!). I'm going to keep it simple.
--========================================================================================================
IF EXISTS(SELECT 1 FROM SYS.EXTERNAL_TABLES T WHERE t.name= 'Squirrel_Census_2018')
    DROP EXTERNAL TABLE dbo.Squirrel_Census_2018
GO
CREATE EXTERNAL TABLE dbo.Squirrel_Census_2018
    WITH (   
        LOCATION = '/demo/nyc_squirrel_census_2018/clean/2018_Central_Park_Squirrel_Census.pqt',  
        DATA_SOURCE = argon_adls,  
        FILE_FORMAT = ParquetFormat  
    )  
AS 
SELECT 
     cast([lon] as decimal(18,6)) as lon
    ,cast([lat] as decimal(18,6)) as lat
    ,cast([squirrel_id] as varchar(14)) as squirrel_id
    ,cast([hectare] as varchar(3)) as hectare
    ,cast([shift] as varchar(2)) as shift
    -- Let's do something with the date. I like YYYY-MM-DD which prevents ambiguity
    ,CAST(substring([sighting_date], 5, 4) --Year
	+ '-' + CASE LEN(RTRIM(LTRIM(substring([sighting_date], 1, 2))))
		WHEN 1
			THEN + '0' + RTRIM(LTRIM(substring([sighting_date], 1, 2)))
		WHEN 2
			THEN + RTRIM(LTRIM(substring([sighting_date], 1, 2)))
		END + '-' + CASE LEN(RTRIM(LTRIM(substring([sighting_date], 1, 2))))
		WHEN 1
			THEN + '0' + RTRIM(LTRIM(substring([sighting_date], 1, 2)))
		WHEN 2
			THEN + RTRIM(LTRIM(substring([sighting_date], 1, 2)))
		END AS DATE) AS sighting_date
    ,cast([hectare_squirrel_number] as varchar(2)) as hectare_squirrel_number
    ,cast([age]as varchar(8)) as age
    ,cast([primary_fur_color] as varchar(8)) as primary_fur_color
    ,cast([highlight_fur_color] as varchar(22)) as highlight_fur_color
    ,cast([comb_primary_highlight_color] as varchar(27)) as comb_primary_highlight_color
    ,cast([color_notes] as varchar(200)) as color_notes
    ,cast([sighting_loc] as varchar(12)) as sighting_loc
    ,cast([above_ground_sighter_meas] as varchar(5)) as above_ground_sighter_meas
    ,cast([specific_location] as varchar(120)) as specific_location
    ,cast([running] as varchar(5)) as running
    ,cast([chasing] as varchar(5)) as chasing
    ,cast([climbing] as varchar(5)) as climbing
    ,cast([eating] as varchar(5)) as eating
    ,cast([foraging] as varchar(5)) as foraging
    ,cast([other_activities] as varchar(150)) as other_activities
    ,cast([kuks] as varchar(5)) as kuks
    ,cast([quaas] as varchar(5)) as quaas
    ,cast([moans] as varchar(5)) as moans
    ,cast([tail_flags] as varchar(5)) as tail_flags
    ,cast([tail_twitches] as varchar(5)) as tail_twitches
    ,cast([approaches] as varchar(5)) as approaches
    ,cast([indifferent] as varchar(5)) as indifferent
    ,cast([runs_from] as varchar(50)) as runs_from
    ,cast([other_Interactions] as varchar(120)) as other_interactions
    -- ,cast([lon_lat] as varchar(45)) as lon_lat - we wont need this. It's already the first two columns and easier to work with.
 FROM [dbo].[squirrel_census]
--========================================================================================================
-- Be sure our select is working from the new table
--========================================================================================================

select * from dbo.Squirrel_Census_2018



-- =====================================================
-- Script 02: Bronze OilPrice → Gold CETAS
-- Runs: Synapse Serverless (LS_Synapse_Serverless)
-- Database: aer_synapse_db
-- ADF Pipeline: pl_oil_to_gold
-- =====================================================

USE aer_synapse_db;

DECLARE @RunDate NVARCHAR(10) = CONVERT(NVARCHAR(10), GETDATE(), 120);
DECLARE @RunDateClean NVARCHAR(8) = REPLACE(@RunDate, '-', '');

-- Drop if re-running (idempotent)
DECLARE @DropSQL NVARCHAR(MAX) = N'
    IF OBJECT_ID(''dbo.fact_oilprice_' + @RunDateClean + N''') IS NOT NULL
        DROP EXTERNAL TABLE dbo.fact_oilprice_' + @RunDateClean + N';';
EXEC sp_executesql @DropSQL;

-- CETAS: Bronze EIA Parquet → Gold OilPrice Parquet
DECLARE @SQL NVARCHAR(MAX) = N'
CREATE EXTERNAL TABLE dbo.fact_oilprice_' + @RunDateClean + N'
WITH (
    LOCATION = ''/gold/fact_oilprice/' + @RunDate + N'/'',
    DATA_SOURCE = aer_adls,
    FILE_FORMAT = parquet_format
) AS
SELECT
    TRY_CAST([period] AS DATE) AS PriceDate,
    TRY_CAST([value] AS DECIMAL(10,2)) AS OilPrice_USD,
    [series-description] AS SeriesDescription,
    GETDATE() AS Loaded_At
FROM OPENROWSET(
    BULK ''/bronze/oilprice/' + @RunDate + N'/*.parquet'',
    DATA_SOURCE = ''aer_adls'',
    FORMAT = ''PARQUET''
) AS b
WHERE [period] IS NOT NULL
  AND [value] IS NOT NULL
  AND TRY_CAST([value] AS DECIMAL(10,2)) IS NOT NULL;';
EXEC sp_executesql @SQL;

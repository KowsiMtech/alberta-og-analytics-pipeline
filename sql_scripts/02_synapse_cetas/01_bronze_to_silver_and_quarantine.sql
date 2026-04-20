-- =====================================================
-- Script 01: Bronze → Silver + Quarantine CETAS
-- Runs: Synapse Serverless (LS_Synapse_Serverless)
-- Database: aer_synapse_db
-- ADF Pipeline: PL_Bronze_to_Silver_quarantine
-- =====================================================

USE aer_synapse_db;

DECLARE @RunDate NVARCHAR(10) = CONVERT(NVARCHAR(10), GETDATE(), 120);
DECLARE @RunDateClean NVARCHAR(8) = REPLACE(@RunDate, '-', '');

-- Drop external tables if re-running same day (idempotent)
DECLARE @DropSQL NVARCHAR(MAX) = N'
    IF OBJECT_ID(''dbo.quarantine_' + @RunDateClean + N''') IS NOT NULL
        DROP EXTERNAL TABLE dbo.quarantine_' + @RunDateClean + N';
    IF OBJECT_ID(''dbo.silver_clean_' + @RunDateClean + N''') IS NOT NULL
        DROP EXTERNAL TABLE dbo.silver_clean_' + @RunDateClean + N';';
EXEC sp_executesql @DropSQL;

-- =====================================================
-- QUARANTINE: Bad rows with RejectReason
-- 3 validation rules:
--   1. NULL_UWI — missing well identifier
--   2. INVALID_DATE — LICENSE_ISSUE_DATE not 8 chars (YYYYMMDD)
--   3. NEGATIVE_DEPTH — WELL_TOTAL_DEPTH < 0
-- =====================================================

DECLARE @QuarantineSQL NVARCHAR(MAX) = N'
CREATE EXTERNAL TABLE dbo.quarantine_' + @RunDateClean + N'
WITH (
    LOCATION = ''/quarantine/well_rejects/' + @RunDate + N'/'',
    DATA_SOURCE = aer_adls,
    FILE_FORMAT = parquet_format
) AS
SELECT
    UWI_DISPLAY_FORMAT AS WellID,
    WELL_NAME AS WellName,
    LICENSE_NO AS LicenceNumber,
    LICENCE_STATUS AS LicenceStatus,
    LICENSE_ISSUE_DATE AS LicenceIssueDate,
    OPERATOR_CODE AS OperatorCode,
    WELL_TOTAL_DEPTH AS WellTotalDepth,
    CASE
        WHEN UWI_DISPLAY_FORMAT IS NULL OR UWI_DISPLAY_FORMAT = '''' THEN ''NULL_UWI''
        WHEN LEN(LICENSE_ISSUE_DATE) <> 8 THEN ''INVALID_DATE''
        WHEN TRY_CAST(WELL_TOTAL_DEPTH AS DECIMAL(10,2)) < 0 THEN ''NEGATIVE_DEPTH''
        ELSE ''OTHER''
    END AS RejectReason,
    GETDATE() AS QuarantineDate
FROM OPENROWSET(
    BULK ''/bronze/well/' + @RunDate + N'/*.parquet'',
    DATA_SOURCE = ''aer_adls'',
    FORMAT = ''PARQUET''
) AS bronze
WHERE UWI_DISPLAY_FORMAT IS NULL OR UWI_DISPLAY_FORMAT = ''''
   OR LEN(LICENSE_ISSUE_DATE) <> 8
   OR TRY_CAST(WELL_TOTAL_DEPTH AS DECIMAL(10,2)) < 0;';
EXEC sp_executesql @QuarantineSQL;

-- =====================================================
-- SILVER CLEAN: Validated, type-cast, cleaned rows
-- =====================================================

DECLARE @SilverSQL NVARCHAR(MAX) = N'
CREATE EXTERNAL TABLE dbo.silver_clean_' + @RunDateClean + N'
WITH (
    LOCATION = ''/silver/clean/' + @RunDate + N'/'',
    DATA_SOURCE = aer_adls,
    FILE_FORMAT = parquet_format
) AS
SELECT
    UWI_DISPLAY_FORMAT AS WellID,
    KEY_LIST_OF_WELLS AS WellKeyID,
    LTRIM(RTRIM(WELL_NAME)) AS WellName,
    FIELD_CODE AS FieldCode,
    POOL_CODE AS PoolCode,
    LICENSE_NO AS LicenceNumber,
    LICENCE_STATUS AS LicenceStatus,
    TRY_CONVERT(DATE, LICENSE_ISSUE_DATE, 112) AS LicenceIssueDate,
    LICENSEE_CODE AS LicenseeCode,
    OPERATOR_CODE AS OperatorCode,
    TRY_CONVERT(DATE, FIN_DRL_DATE, 112) AS FinDrlDate,
    TRY_CAST(WELL_TOTAL_DEPTH AS DECIMAL(10,2)) AS WellTotalDepth,
    WELL_STAT_CODE AS WellStatCode,
    TRY_CONVERT(DATE, WELL_STAT_DATE, 112) AS WellStatDate,
    CASE WHEN FLUID_SHORT_DESC = ''N/A'' THEN NULL
         ELSE LTRIM(RTRIM(FLUID_SHORT_DESC)) END AS FluidDesc,
    CASE WHEN MODE_SHORT_DESC = ''N/A'' THEN NULL
         ELSE LTRIM(RTRIM(MODE_SHORT_DESC)) END AS ModeDesc,
    CASE WHEN TYPE_SHORT_DESC = ''N/A'' THEN NULL
         ELSE LTRIM(RTRIM(TYPE_SHORT_DESC)) END AS TypeDesc,
    CASE WHEN STRUCTURE_SHORT_DESC = ''N/A'' THEN NULL
         ELSE LTRIM(RTRIM(STRUCTURE_SHORT_DESC)) END AS StructureDesc,
    ''BATCH_' + @RunDate + N''' AS BatchID,
    GETDATE() AS IngestionDate,
    ''WellList.txt'' AS SourceFile
FROM OPENROWSET(
    BULK ''/bronze/well/' + @RunDate + N'/*.parquet'',
    DATA_SOURCE = ''aer_adls'',
    FORMAT = ''PARQUET''
) AS bronze
WHERE UWI_DISPLAY_FORMAT IS NOT NULL AND UWI_DISPLAY_FORMAT <> ''''
  AND LEN(LICENSE_ISSUE_DATE) = 8
  AND TRY_CAST(WELL_TOTAL_DEPTH AS DECIMAL(10,2)) >= 0;';
EXEC sp_executesql @SilverSQL;

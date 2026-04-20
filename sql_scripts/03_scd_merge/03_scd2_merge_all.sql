-- =====================================================
-- Script 03: SCD Type 2 MERGE — TRUE SCD2 in Azure SQL DB
-- Runs: Azure SQL DB (LS_AzureSQL)
-- Database: aer-oilgas-gold-db
-- ADF Pipeline: PL_SCD2_MERGE
--
-- WHY AZURE SQL (not Synapse):
-- Synapse Serverless cannot UPDATE Parquet files (immutable).
-- SCD2 requires row-level UPDATE to expire old rows.
-- Azure SQL DB native T-SQL MERGE at $7/month handles this perfectly.
-- =====================================================

DECLARE @BatchDate DATE = CAST(GETDATE() AS DATE);

-- =====================================================
-- SCD TYPE 2: Dim_Operator
-- Step A: Expire rows where LicenseeCode changed
-- =====================================================
UPDATE t
SET t.[Is_Current] = 0,
    t.Effective_To = @BatchDate,
    t.Updated_At = GETDATE()
FROM gold.Dim_Operator t
INNER JOIN (
    SELECT DISTINCT OperatorCode, LicenseeCode
    FROM staging.Silver_Wells
    WHERE OperatorCode IS NOT NULL AND OperatorCode <> ''
) s ON t.OperatorCode = s.OperatorCode AND t.[Is_Current] = 1
WHERE ISNULL(t.LicenseeCode,'') <> ISNULL(s.LicenseeCode,'');

-- Step B: Insert new current rows (composite key: OperatorCode + LicenseeCode)
-- Real Alberta data: operators like Cenovus (0Z5F0) have 9+ licensees
INSERT INTO gold.Dim_Operator
    (OperatorCode, LicenseeCode, Effective_From, Effective_To, [Is_Current])
SELECT DISTINCT s.OperatorCode, s.LicenseeCode,
    @BatchDate, '9999-12-31', 1
FROM staging.Silver_Wells s
WHERE s.OperatorCode IS NOT NULL AND s.OperatorCode <> ''
  AND NOT EXISTS (
      SELECT 1 FROM gold.Dim_Operator d
      WHERE d.OperatorCode = s.OperatorCode
        AND d.LicenseeCode = s.LicenseeCode  -- composite key prevents duplicates
        AND d.[Is_Current] = 1);

-- =====================================================
-- SCD TYPE 2: Dim_Well
-- Step A: Expire rows where operator/status/mode changed
-- =====================================================
UPDATE t
SET t.[Is_Current] = 0,
    t.Effective_To = @BatchDate,
    t.Updated_At = GETDATE()
FROM gold.Dim_Well t
INNER JOIN staging.Silver_Wells s
    ON t.WellID = s.WellID AND t.[Is_Current] = 1
WHERE ISNULL(t.OperatorCode,'') <> ISNULL(s.OperatorCode,'')
   OR ISNULL(t.WellStatCode,'') <> ISNULL(s.WellStatCode,'')
   OR ISNULL(t.ModeDesc,'') <> ISNULL(s.ModeDesc,'');

-- Step B: Insert new current rows
INSERT INTO gold.Dim_Well
    (WellID, WellName, FieldCode, LicenseeCode, OperatorCode,
     WellTotalDepth, WellStatCode, FluidDesc, ModeDesc,
     Effective_From, Effective_To, [Is_Current])
SELECT s.WellID, s.WellName, s.FieldCode, s.LicenseeCode, s.OperatorCode,
       s.WellTotalDepth, s.WellStatCode, s.FluidDesc, s.ModeDesc,
       @BatchDate, '9999-12-31', 1
FROM staging.Silver_Wells s
WHERE NOT EXISTS (
    SELECT 1 FROM gold.Dim_Well d
    WHERE d.WellID = s.WellID AND d.[Is_Current] = 1);

-- =====================================================
-- SCD TYPE 1: Dim_LicenceStatus (UPSERT)
-- =====================================================
MERGE gold.Dim_LicenceStatus AS t
USING (
    SELECT DISTINCT LicenceStatus
    FROM staging.Silver_Wells
    WHERE LicenceStatus IS NOT NULL
) AS s
ON t.LicenceStatus = s.LicenceStatus
WHEN NOT MATCHED THEN
    INSERT (LicenceStatus) VALUES (s.LicenceStatus);

-- =====================================================
-- SCD TYPE 1: Dim_WellType (UPSERT)
-- =====================================================
MERGE gold.Dim_WellType AS t
USING (
    SELECT DISTINCT FluidDesc, ModeDesc, TypeDesc, StructureDesc
    FROM staging.Silver_Wells
) AS s
ON ISNULL(t.FluidDesc,'') = ISNULL(s.FluidDesc,'')
   AND ISNULL(t.ModeDesc,'') = ISNULL(s.ModeDesc,'')
   AND ISNULL(t.TypeDesc,'') = ISNULL(s.TypeDesc,'')
   AND ISNULL(t.StructureDesc,'') = ISNULL(s.StructureDesc,'')
WHEN NOT MATCHED THEN
    INSERT (FluidDesc, ModeDesc, TypeDesc, StructureDesc)
    VALUES (s.FluidDesc, s.ModeDesc, s.TypeDesc, s.StructureDesc);

-- =====================================================
-- Fact_OilPrice: UPSERT from staging.OilPrice
-- =====================================================
MERGE gold.Fact_OilPrice AS t
USING (
    SELECT
        TRY_CAST(period AS DATE) AS PriceDate,
        TRY_CAST(value AS DECIMAL(10,2)) AS OilPrice_USD,
        [series-description] AS SeriesDescription
    FROM staging.OilPrice
    WHERE period IS NOT NULL AND value IS NOT NULL
) AS s
ON t.PriceDate = s.PriceDate
WHEN MATCHED THEN UPDATE SET
    t.OilPrice_USD = s.OilPrice_USD,
    t.Updated_At = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (PriceDate, OilPrice_USD, SeriesDescription)
    VALUES (s.PriceDate, s.OilPrice_USD, s.SeriesDescription);

-- =====================================================
-- Fact_WellSnapshot: daily reload
-- =====================================================
TRUNCATE TABLE gold.Fact_WellSnapshot;

INSERT INTO gold.Fact_WellSnapshot
    (SnapshotDate, WellID, OperatorCode, LicenceStatus,
     FluidDesc, ModeDesc, FieldCode, WellTotalDepth)
SELECT @BatchDate, WellID, OperatorCode, LicenceStatus,
       FluidDesc, ModeDesc, FieldCode, WellTotalDepth
FROM staging.Silver_Wells
WHERE WellID IS NOT NULL;

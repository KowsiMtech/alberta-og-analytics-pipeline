-- =====================================================
-- Validation Queries
-- Run after SCD2 MERGE to verify pipeline success
-- Database: aer-oilgas-gold-db (Azure SQL DB)
-- =====================================================

-- =====================================================
-- ROW COUNT VALIDATION
-- =====================================================
SELECT 'staging.Silver_Wells' AS TableName, COUNT(*) AS RowCount
FROM staging.Silver_Wells
UNION ALL
SELECT 'staging.OilPrice', COUNT(*) FROM staging.OilPrice
UNION ALL
SELECT 'gold.Dim_Operator (Total)', COUNT(*) FROM gold.Dim_Operator
UNION ALL
SELECT 'gold.Dim_Operator (Current)', COUNT(*)
FROM gold.Dim_Operator WHERE [Is_Current] = 1
UNION ALL
SELECT 'gold.Dim_Operator (Expired)', COUNT(*)
FROM gold.Dim_Operator WHERE [Is_Current] = 0
UNION ALL
SELECT 'gold.Dim_Well (Total)', COUNT(*) FROM gold.Dim_Well
UNION ALL
SELECT 'gold.Dim_Well (Current)', COUNT(*)
FROM gold.Dim_Well WHERE [Is_Current] = 1
UNION ALL
SELECT 'gold.Dim_LicenceStatus', COUNT(*) FROM gold.Dim_LicenceStatus
UNION ALL
SELECT 'gold.Dim_WellType', COUNT(*) FROM gold.Dim_WellType
UNION ALL
SELECT 'gold.Fact_OilPrice', COUNT(*) FROM gold.Fact_OilPrice
UNION ALL
SELECT 'gold.Fact_WellSnapshot', COUNT(*) FROM gold.Fact_WellSnapshot;

-- =====================================================
-- SCD2 PROOF — Operators with Multiple Licensees
-- Expected: ~252 operators (10.21% of 2,469)
-- =====================================================
SELECT OperatorCode, COUNT(DISTINCT LicenseeCode) AS LicenseeCount
FROM gold.Dim_Operator
WHERE [Is_Current] = 1
GROUP BY OperatorCode
HAVING COUNT(DISTINCT LicenseeCode) > 1
ORDER BY LicenseeCount DESC;

-- =====================================================
-- CENOVUS EXAMPLE — 9+ licensees under one operator
-- =====================================================
SELECT OperatorCode, LicenseeCode, [Is_Current],
       Effective_From, Effective_To
FROM gold.Dim_Operator
WHERE OperatorCode = '0Z5F0'
ORDER BY LicenseeCode;

-- =====================================================
-- SCD Type 2 Demo Script
-- Proves SCD2 history tracking works end-to-end
-- Database: aer-oilgas-gold-db
-- =====================================================

-- =====================================================
-- STEP 1: Check BEFORE state
-- =====================================================
SELECT OperatorCode, LicenseeCode, [Is_Current], Effective_From, Effective_To
FROM gold.Dim_Operator
WHERE OperatorCode = '0Z5F0' AND LicenseeCode = '0HE90';

-- Expected: 1 row with Is_Current=True, Effective_To=9999-12-31

-- =====================================================
-- STEP 2: Simulate M&A change — rename licensee in staging
-- =====================================================
UPDATE staging.Silver_Wells
SET LicenseeCode = 'NEWCO'
WHERE OperatorCode = '0Z5F0' AND LicenseeCode = '0HE90';

-- =====================================================
-- STEP 3: Re-run PL_SCD2_MERGE pipeline
-- (Or run the SCD2 MERGE script manually)
-- =====================================================

-- =====================================================
-- STEP 4: Check AFTER state — should see 2 rows
-- =====================================================
SELECT OperatorCode, LicenseeCode, [Is_Current], Effective_From, Effective_To
FROM gold.Dim_Operator
WHERE OperatorCode = '0Z5F0'
AND LicenseeCode IN ('0HE90', 'NEWCO')
ORDER BY Effective_From;

-- Expected Output (SCD2 PROOF):
-- Row 1: 0Z5F0, 0HE90, Is_Current=False, Effective_To=<today>  ← EXPIRED
-- Row 2: 0Z5F0, NEWCO, Is_Current=True,  Effective_To=9999-12-31 ← NEW CURRENT

-- =====================================================
-- STEP 5 (Optional): Revert the change
-- =====================================================
-- UPDATE staging.Silver_Wells
-- SET LicenseeCode = '0HE90'
-- WHERE OperatorCode = '0Z5F0' AND LicenseeCode = 'NEWCO';

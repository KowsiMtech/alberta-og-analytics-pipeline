-- =====================================================
-- Alberta Oil & Gas Analytics Pipeline
-- Azure SQL DB DDL Script
-- Database: aer-oilgas-gold-db
-- Author: Kowsalya Gopinathan
-- =====================================================

-- Create schemas
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging')
    EXEC('CREATE SCHEMA staging');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

-- =====================================================
-- STAGING TABLES (transient — TRUNCATE+RELOAD each run)
-- =====================================================

IF OBJECT_ID('staging.Silver_Wells', 'U') IS NOT NULL
    DROP TABLE staging.Silver_Wells;

CREATE TABLE staging.Silver_Wells (
    WellID NVARCHAR(30),
    WellKeyID NVARCHAR(20),
    WellName NVARCHAR(50),
    FieldCode NVARCHAR(10),
    PoolCode NVARCHAR(10),
    LicenceNumber NVARCHAR(15),
    LicenceStatus NVARCHAR(20),
    LicenceIssueDate DATE,
    LicenseeCode NVARCHAR(10),
    OperatorCode NVARCHAR(10),
    FinDrlDate DATE,
    WellTotalDepth DECIMAL(10,2),
    WellStatCode NVARCHAR(15),
    WellStatDate DATE,
    FluidDesc NVARCHAR(25),
    ModeDesc NVARCHAR(25),
    TypeDesc NVARCHAR(25),
    StructureDesc NVARCHAR(25),
    BatchID NVARCHAR(50),
    IngestionDate DATETIME2
);
GO

IF OBJECT_ID('staging.OilPrice', 'U') IS NOT NULL
    DROP TABLE staging.OilPrice;

CREATE TABLE staging.OilPrice (
    period NVARCHAR(20),
    value NVARCHAR(20),
    [series-description] NVARCHAR(200),
    [product-name] NVARCHAR(100),
    units NVARCHAR(50),
    IngestionDate DATETIME2
);
GO

-- =====================================================
-- GOLD DIMENSIONAL MODEL
-- =====================================================

-- Dim_Operator — SCD Type 2 (tracks operator-licensee changes)
IF OBJECT_ID('gold.Dim_Operator', 'U') IS NOT NULL
    DROP TABLE gold.Dim_Operator;

CREATE TABLE gold.Dim_Operator (
    Operator_SK INT IDENTITY(1,1) PRIMARY KEY,
    OperatorCode NVARCHAR(10) NOT NULL,
    LicenseeCode NVARCHAR(10),
    Effective_From DATE NOT NULL DEFAULT GETDATE(),
    Effective_To DATE DEFAULT '9999-12-31',
    [Is_Current] BIT NOT NULL DEFAULT 1,
    Created_At DATETIME2 DEFAULT GETDATE(),
    Updated_At DATETIME2 DEFAULT GETDATE()
);

CREATE INDEX IX_Dim_Operator_OperatorCode
    ON gold.Dim_Operator(OperatorCode)
    INCLUDE (LicenseeCode, [Is_Current]);
GO

-- Dim_Well — SCD Type 2 (tracks well attribute changes)
IF OBJECT_ID('gold.Dim_Well', 'U') IS NOT NULL
    DROP TABLE gold.Dim_Well;

CREATE TABLE gold.Dim_Well (
    Well_SK INT IDENTITY(1,1) PRIMARY KEY,
    WellID NVARCHAR(30) NOT NULL,
    WellName NVARCHAR(50),
    FieldCode NVARCHAR(10),
    LicenseeCode NVARCHAR(10),
    OperatorCode NVARCHAR(10),
    WellTotalDepth DECIMAL(10,2),
    WellStatCode NVARCHAR(15),
    FluidDesc NVARCHAR(25),
    ModeDesc NVARCHAR(25),
    Effective_From DATE NOT NULL DEFAULT GETDATE(),
    Effective_To DATE DEFAULT '9999-12-31',
    [Is_Current] BIT NOT NULL DEFAULT 1,
    Created_At DATETIME2 DEFAULT GETDATE(),
    Updated_At DATETIME2 DEFAULT GETDATE()
);

CREATE INDEX IX_Dim_Well_WellID
    ON gold.Dim_Well(WellID)
    INCLUDE ([Is_Current]);
GO

-- Dim_LicenceStatus — SCD Type 1 (simple upsert)
IF OBJECT_ID('gold.Dim_LicenceStatus', 'U') IS NOT NULL
    DROP TABLE gold.Dim_LicenceStatus;

CREATE TABLE gold.Dim_LicenceStatus (
    LicenceStatus_SK INT IDENTITY(1,1) PRIMARY KEY,
    LicenceStatus NVARCHAR(20) NOT NULL UNIQUE,
    Updated_At DATETIME2 DEFAULT GETDATE()
);
GO

-- Dim_WellType — SCD Type 1 (simple upsert)
IF OBJECT_ID('gold.Dim_WellType', 'U') IS NOT NULL
    DROP TABLE gold.Dim_WellType;

CREATE TABLE gold.Dim_WellType (
    WellType_SK INT IDENTITY(1,1) PRIMARY KEY,
    FluidDesc NVARCHAR(25),
    ModeDesc NVARCHAR(25),
    TypeDesc NVARCHAR(25),
    StructureDesc NVARCHAR(25),
    Updated_At DATETIME2 DEFAULT GETDATE()
);
GO

-- Fact_OilPrice — UPSERT on PriceDate
IF OBJECT_ID('gold.Fact_OilPrice', 'U') IS NOT NULL
    DROP TABLE gold.Fact_OilPrice;

CREATE TABLE gold.Fact_OilPrice (
    OilPrice_SK INT IDENTITY(1,1) PRIMARY KEY,
    PriceDate DATE NOT NULL UNIQUE,
    OilPrice_USD DECIMAL(10,2) NOT NULL,
    SeriesDescription NVARCHAR(200),
    Created_At DATETIME2 DEFAULT GETDATE(),
    Updated_At DATETIME2 DEFAULT GETDATE()
);
GO

-- Fact_WellSnapshot — daily reload
IF OBJECT_ID('gold.Fact_WellSnapshot', 'U') IS NOT NULL
    DROP TABLE gold.Fact_WellSnapshot;

CREATE TABLE gold.Fact_WellSnapshot (
    Snapshot_SK INT IDENTITY(1,1) PRIMARY KEY,
    SnapshotDate DATE NOT NULL,
    WellID NVARCHAR(30),
    OperatorCode NVARCHAR(10),
    LicenceStatus NVARCHAR(20),
    FluidDesc NVARCHAR(25),
    ModeDesc NVARCHAR(25),
    FieldCode NVARCHAR(10),
    WellTotalDepth DECIMAL(10,2),
    Created_At DATETIME2 DEFAULT GETDATE()
);

CREATE INDEX IX_Fact_WellSnapshot_OperatorCode
    ON gold.Fact_WellSnapshot(OperatorCode, SnapshotDate);
GO

-- =====================================================
-- ANALYTICAL VIEW
-- =====================================================

IF OBJECT_ID('gold.vw_WellMarketTrend', 'V') IS NOT NULL
    DROP VIEW gold.vw_WellMarketTrend;
GO

CREATE VIEW gold.vw_WellMarketTrend AS
SELECT
    ws.SnapshotDate,
    op.OilPrice_USD AS LatestOilPrice_USD,
    dop.OperatorCode,
    COUNT(DISTINCT ws.WellID) AS TotalWells,
    AVG(ws.WellTotalDepth) AS AvgWellDepth
FROM gold.Fact_WellSnapshot ws
LEFT JOIN gold.Fact_OilPrice op ON op.PriceDate = ws.SnapshotDate
LEFT JOIN gold.Dim_Operator dop ON dop.OperatorCode = ws.OperatorCode
    AND dop.[Is_Current] = 1
GROUP BY ws.SnapshotDate, op.OilPrice_USD, dop.OperatorCode;
GO

PRINT 'All tables and view created successfully';

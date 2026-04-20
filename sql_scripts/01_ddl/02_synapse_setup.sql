-- =====================================================
-- Synapse Serverless One-Time Setup
-- Database: aer_synapse_db (in aer-synapse-ws workspace)
-- Connect: aer-synapse-ws-ondemand.sql.azuresynapse.net
-- =====================================================

-- Create database (run in master first)
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'aer_synapse_db')
    CREATE DATABASE aer_synapse_db;
GO

USE aer_synapse_db;
GO

-- Master key (required for external data source credentials)
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'UsE_S3cur3_P@ssw0rd_H3re_!23';
GO

-- Database-scoped credential using Managed Identity
IF NOT EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'aer_managed_identity')
    CREATE DATABASE SCOPED CREDENTIAL aer_managed_identity
    WITH IDENTITY = 'Managed Identity';
GO

-- External data source pointing to ADLS container
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'aer_adls')
    CREATE EXTERNAL DATA SOURCE aer_adls
    WITH (
        LOCATION = 'abfss://aer-og-data@aeroilgasstorage.dfs.core.windows.net',
        CREDENTIAL = aer_managed_identity
    );
GO

-- External file format for Parquet
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'parquet_format')
    CREATE EXTERNAL FILE FORMAT parquet_format
    WITH (FORMAT_TYPE = PARQUET);
GO


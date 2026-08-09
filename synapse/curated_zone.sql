CREATE DATABASE etl_db;
GO

USE etl_db;
GO

-- Credential so Synapse can read ADLS via Managed Identity
CREATE DATABASE SCOPED CREDENTIAL adls_credential
WITH IDENTITY = 'Managed Identity';

-- External data source pointing to the curated zone
CREATE EXTERNAL DATA SOURCE curated_zone
WITH (
  LOCATION   = 'https://saetlpipeline.dfs.core.windows.net/datalake/curated/',
  CREDENTIAL = adls_credential
);
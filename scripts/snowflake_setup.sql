-- =============================================================================
-- Snowflake bootstrap for credit-risk-pipeline. Idempotent; run as ACCOUNTADMIN.
-- Run once per environment: set the environment below to DEV, then again to PROD.
-- No credentials live here. Grant the role to your own user at the bottom.
-- =============================================================================

SET env_name = 'DEV';                                    -- 'DEV' or 'PROD'
SET db_name  = 'CREDIT_RISK_' || $env_name;

USE ROLE ACCOUNTADMIN;

-- 1. Role -------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS CREDIT_RISK_ENGINEER
    COMMENT = 'Loads RAW and runs dbt for the credit risk pipeline';

-- 2. Warehouse: XS, suspends after 60 s idle, resumes on demand -------------
CREATE WAREHOUSE IF NOT EXISTS CREDIT_RISK_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Credit risk pipeline compute';

-- Cost guardrail: hard stop at 10 credits per month.
CREATE RESOURCE MONITOR IF NOT EXISTS CREDIT_RISK_MONITOR
    WITH CREDIT_QUOTA = 10 FREQUENCY = MONTHLY START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 80 PERCENT DO NOTIFY
             ON 100 PERCENT DO SUSPEND;
ALTER WAREHOUSE CREDIT_RISK_WH SET RESOURCE_MONITOR = CREDIT_RISK_MONITOR;

-- 3. Database and layer schemas ----------------------------------------------
CREATE DATABASE IF NOT EXISTS IDENTIFIER($db_name);
USE DATABASE IDENTIFIER($db_name);
CREATE SCHEMA IF NOT EXISTS RAW     COMMENT = 'Source data as delivered (VARCHAR + audit columns)';
CREATE SCHEMA IF NOT EXISTS STAGING COMMENT = 'Typed, cleansed stg_* and int_* models';
CREATE SCHEMA IF NOT EXISTS MARTS   COMMENT = 'Business-ready facts, dimensions and risk marts';

-- 4. Privileges ---------------------------------------------------------------
GRANT USAGE, OPERATE ON WAREHOUSE CREDIT_RISK_WH TO ROLE CREDIT_RISK_ENGINEER;
GRANT ALL PRIVILEGES ON DATABASE IDENTIFIER($db_name) TO ROLE CREDIT_RISK_ENGINEER;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE IDENTIFIER($db_name) TO ROLE CREDIT_RISK_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE IDENTIFIER($db_name) TO ROLE CREDIT_RISK_ENGINEER;

-- 5. Attach the role to your user (replace the placeholder, then uncomment). ----
-- GRANT ROLE CREDIT_RISK_ENGINEER TO USER <your_user>;
-- Key-pair auth (recommended):
-- ALTER USER <your_user> SET RSA_PUBLIC_KEY = '<public key body, no header/footer>';

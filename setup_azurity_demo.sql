-- =============================================================================
-- AZURITY PHARMACEUTICALS ML DEMO - INFRASTRUCTURE SETUP
-- =============================================================================
-- Run this script with ACCOUNTADMIN role to set up:
--   1. Database and schemas
--   2. Compute pool for Container Runtime
--   3. External Access Integration for pip install
--   4. Warehouse for SQL inference
--   5. Git repository integration
--   6. Synthetic data tables
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- 1. CREATE DATABASE AND SCHEMAS
-- =============================================================================
CREATE DATABASE IF NOT EXISTS AZURITY_DEMO_DB;
USE DATABASE AZURITY_DEMO_DB;

CREATE SCHEMA IF NOT EXISTS ML COMMENT = 'ML models, feature store, and training data';
CREATE SCHEMA IF NOT EXISTS RAW COMMENT = 'Raw synthetic data for demo';

-- =============================================================================
-- 2. CREATE COMPUTE POOL FOR CONTAINER RUNTIME NOTEBOOKS
-- =============================================================================
CREATE COMPUTE POOL IF NOT EXISTS AZURITY_ML_POOL
    MIN_NODES = 1
    MAX_NODES = 2
    INSTANCE_FAMILY = HIGHMEM_X64_S
    AUTO_SUSPEND_SECS = 600
    COMMENT = 'Compute pool for Azurity ML notebooks in Container Runtime';

-- =============================================================================
-- 3. CREATE EXTERNAL ACCESS INTEGRATION FOR PIP INSTALL
-- =============================================================================
CREATE OR REPLACE NETWORK RULE AZURITY_PYPI_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('pypi.org', 'files.pythonhosted.org', 'pypi.python.org');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION AZURITY_PYPI_EAI
    ALLOWED_NETWORK_RULES = (AZURITY_PYPI_RULE)
    ENABLED = TRUE
    COMMENT = 'External access for pip install in Azurity notebooks';

-- =============================================================================
-- 4. CREATE WAREHOUSE FOR SQL INFERENCE
-- =============================================================================
CREATE WAREHOUSE IF NOT EXISTS AZURITY_ML_WH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse for Azurity ML inference and queries';

USE WAREHOUSE AZURITY_ML_WH;

-- =============================================================================
-- 5. GIT REPOSITORY INTEGRATION (Optional - if not already set up)
-- =============================================================================
-- Note: You may need to create an API integration first if not already done
-- CREATE OR REPLACE API INTEGRATION GITHUB_API_INTEGRATION
--     API_PROVIDER = git_https_api
--     API_ALLOWED_PREFIXES = ('https://github.com/sfc-gh-eescobar/')
--     ENABLED = TRUE;

-- CREATE OR REPLACE GIT REPOSITORY AZURITY_ML_REPO
--     API_INTEGRATION = GITHUB_API_INTEGRATION
--     ORIGIN = 'https://github.com/sfc-gh-eescobar/azurity-ml-demo';

-- =============================================================================
-- 6. CREATE SYNTHETIC DATA TABLES
-- =============================================================================
USE SCHEMA RAW;

-- Products table - Azurity's actual product portfolio
CREATE OR REPLACE TABLE PRODUCTS (
    PRODUCT_ID VARCHAR(10) PRIMARY KEY,
    PRODUCT_NAME VARCHAR(100),
    GENERIC_NAME VARCHAR(100),
    THERAPEUTIC_AREA VARCHAR(50),
    DOSAGE_FORM VARCHAR(50),
    FDA_APPROVAL_DATE DATE,
    IS_PEDIATRIC BOOLEAN,
    LAUNCH_DATE DATE
);

INSERT INTO PRODUCTS VALUES
    ('P001', 'Eprontia', 'Topiramate', 'Neurology', 'Oral Solution', '2022-01-15', TRUE, '2022-03-01'),
    ('P002', 'Katerzia', 'Amlodipine', 'Cardiovascular', 'Oral Suspension', '2019-06-20', TRUE, '2019-09-01'),
    ('P003', 'Qbrelis', 'Lisinopril', 'Cardiovascular', 'Oral Solution', '2016-04-15', TRUE, '2016-07-01'),
    ('P004', 'Zonisade', 'Zonisamide', 'Neurology', 'Oral Suspension', '2021-08-10', TRUE, '2021-11-01'),
    ('P005', 'Arynta', 'Lisdexamfetamine', 'CNS', 'Oral Solution', '2025-02-28', TRUE, '2025-05-01');

-- Healthcare Providers (HCPs) table
CREATE OR REPLACE TABLE HEALTHCARE_PROVIDERS AS
WITH hcp_generator AS (
    SELECT 
        'HCP' || LPAD(SEQ4()::VARCHAR, 6, '0') AS HCP_ID,
        CASE MOD(SEQ4(), 10)
            WHEN 0 THEN 'Pediatric Neurology'
            WHEN 1 THEN 'Pediatric Cardiology'
            WHEN 2 THEN 'General Pediatrics'
            WHEN 3 THEN 'Family Medicine'
            WHEN 4 THEN 'Pediatric Nephrology'
            WHEN 5 THEN 'Pediatric Epileptologist'
            WHEN 6 THEN 'Child Psychiatry'
            WHEN 7 THEN 'Internal Medicine'
            WHEN 8 THEN 'Neurology'
            WHEN 9 THEN 'Cardiology'
        END AS SPECIALTY,
        CASE 
            WHEN UNIFORM(0, 100, RANDOM()) < 10 THEN 10  -- Top 10% = Decile 10
            WHEN UNIFORM(0, 100, RANDOM()) < 25 THEN 9
            WHEN UNIFORM(0, 100, RANDOM()) < 40 THEN 8
            WHEN UNIFORM(0, 100, RANDOM()) < 55 THEN 7
            WHEN UNIFORM(0, 100, RANDOM()) < 70 THEN 6
            WHEN UNIFORM(0, 100, RANDOM()) < 80 THEN 5
            WHEN UNIFORM(0, 100, RANDOM()) < 88 THEN 4
            WHEN UNIFORM(0, 100, RANDOM()) < 94 THEN 3
            WHEN UNIFORM(0, 100, RANDOM()) < 98 THEN 2
            ELSE 1
        END AS DECILE,
        CASE MOD(SEQ4(), 50)
            WHEN 0 THEN 'CA' WHEN 1 THEN 'TX' WHEN 2 THEN 'FL' WHEN 3 THEN 'NY' WHEN 4 THEN 'PA'
            WHEN 5 THEN 'IL' WHEN 6 THEN 'OH' WHEN 7 THEN 'GA' WHEN 8 THEN 'NC' WHEN 9 THEN 'MI'
            WHEN 10 THEN 'NJ' WHEN 11 THEN 'VA' WHEN 12 THEN 'WA' WHEN 13 THEN 'AZ' WHEN 14 THEN 'MA'
            WHEN 15 THEN 'TN' WHEN 16 THEN 'IN' WHEN 17 THEN 'MO' WHEN 18 THEN 'MD' WHEN 19 THEN 'WI'
            WHEN 20 THEN 'CO' WHEN 21 THEN 'MN' WHEN 22 THEN 'SC' WHEN 23 THEN 'AL' WHEN 24 THEN 'LA'
            WHEN 25 THEN 'KY' WHEN 26 THEN 'OR' WHEN 27 THEN 'OK' WHEN 28 THEN 'CT' WHEN 29 THEN 'UT'
            WHEN 30 THEN 'IA' WHEN 31 THEN 'NV' WHEN 32 THEN 'AR' WHEN 33 THEN 'MS' WHEN 34 THEN 'KS'
            WHEN 35 THEN 'NM' WHEN 36 THEN 'NE' WHEN 37 THEN 'ID' WHEN 38 THEN 'WV' WHEN 39 THEN 'HI'
            WHEN 40 THEN 'NH' WHEN 41 THEN 'ME' WHEN 42 THEN 'RI' WHEN 43 THEN 'MT' WHEN 44 THEN 'DE'
            WHEN 45 THEN 'SD' WHEN 46 THEN 'ND' WHEN 47 THEN 'AK' WHEN 48 THEN 'VT' ELSE 'WY'
        END AS STATE,
        CASE MOD(SEQ4(), 8)
            WHEN 0 THEN 'Northeast'
            WHEN 1 THEN 'Southeast'
            WHEN 2 THEN 'Midwest'
            WHEN 3 THEN 'Southwest'
            WHEN 4 THEN 'West'
            WHEN 5 THEN 'Northeast'
            WHEN 6 THEN 'Southeast'
            ELSE 'Midwest'
        END AS REGION,
        UNIFORM(0, 12, RANDOM()) AS ANNUAL_CALL_FREQUENCY,
        UNIFORM(0, 24, RANDOM()) AS ANNUAL_EMAIL_FREQUENCY,
        DATEADD('day', -UNIFORM(365, 2000, RANDOM()), CURRENT_DATE()) AS FIRST_RX_DATE
    FROM TABLE(GENERATOR(ROWCOUNT => 10000))
)
SELECT * FROM hcp_generator;

-- Prescriptions table - 2+ years of prescription data
CREATE OR REPLACE TABLE PRESCRIPTIONS AS
WITH date_range AS (
    SELECT DATEADD('day', SEQ4(), '2024-01-01')::DATE AS FILL_DATE
    FROM TABLE(GENERATOR(ROWCOUNT => 800))
    WHERE DATEADD('day', SEQ4(), '2024-01-01') <= CURRENT_DATE()
),
product_hcp_combos AS (
    SELECT 
        p.PRODUCT_ID,
        h.HCP_ID,
        h.DECILE,
        h.REGION,
        h.SPECIALTY,
        p.THERAPEUTIC_AREA
    FROM PRODUCTS p
    CROSS JOIN (SELECT DISTINCT HCP_ID, DECILE, REGION, SPECIALTY FROM HEALTHCARE_PROVIDERS) h
    WHERE (p.THERAPEUTIC_AREA = 'Neurology' AND h.SPECIALTY IN ('Pediatric Neurology', 'Neurology', 'Pediatric Epileptologist', 'General Pediatrics'))
       OR (p.THERAPEUTIC_AREA = 'Cardiovascular' AND h.SPECIALTY IN ('Pediatric Cardiology', 'Cardiology', 'General Pediatrics', 'Pediatric Nephrology'))
       OR (p.THERAPEUTIC_AREA = 'CNS' AND h.SPECIALTY IN ('Child Psychiatry', 'Pediatric Neurology', 'General Pediatrics', 'Family Medicine'))
),
rx_generator AS (
    SELECT 
        'RX' || LPAD(ROW_NUMBER() OVER (ORDER BY RANDOM())::VARCHAR, 10, '0') AS RX_ID,
        phc.PRODUCT_ID,
        phc.HCP_ID,
        dr.FILL_DATE,
        phc.REGION,
        -- Quantity based on decile (higher decile = more prescriptions)
        GREATEST(1, FLOOR(phc.DECILE * UNIFORM(1, 5, RANDOM()) * 
            -- Seasonality: higher in winter for neuro, higher in spring for cardio
            CASE 
                WHEN phc.THERAPEUTIC_AREA = 'Neurology' AND MONTH(dr.FILL_DATE) IN (11, 12, 1, 2) THEN 1.3
                WHEN phc.THERAPEUTIC_AREA = 'Cardiovascular' AND MONTH(dr.FILL_DATE) IN (3, 4, 5) THEN 1.2
                ELSE 1.0
            END *
            -- Growth trend over time
            (1 + 0.001 * DATEDIFF('day', '2024-01-01', dr.FILL_DATE))
        )) AS QUANTITY,
        CASE UNIFORM(0, 3, RANDOM())
            WHEN 0 THEN 'Commercial'
            WHEN 1 THEN 'Medicare'
            WHEN 2 THEN 'Medicaid'
            ELSE 'Cash'
        END AS PAYER_TYPE
    FROM product_hcp_combos phc
    CROSS JOIN date_range dr
    WHERE UNIFORM(0, 100, RANDOM()) < (phc.DECILE * 2 + 5) -- Higher probability for higher decile HCPs
)
SELECT * FROM rx_generator;

-- Email Campaigns table for A/B testing demo
CREATE OR REPLACE TABLE EMAIL_CAMPAIGNS AS
WITH campaign_dates AS (
    SELECT 
        'CAMP' || LPAD(SEQ4()::VARCHAR, 4, '0') AS CAMPAIGN_ID,
        CASE MOD(SEQ4(), 5)
            WHEN 0 THEN 'P001'
            WHEN 1 THEN 'P002'
            WHEN 2 THEN 'P003'
            WHEN 3 THEN 'P004'
            ELSE 'P005'
        END AS PRODUCT_ID,
        CASE MOD(SEQ4(), 4)
            WHEN 0 THEN 'Product Launch'
            WHEN 1 THEN 'Clinical Data'
            WHEN 2 THEN 'Dosing Reminder'
            ELSE 'Patient Support'
        END AS CAMPAIGN_TYPE,
        DATEADD('day', SEQ4() * 14, '2025-01-01')::DATE AS CAMPAIGN_START_DATE
    FROM TABLE(GENERATOR(ROWCOUNT => 30))
    WHERE DATEADD('day', SEQ4() * 14, '2025-01-01') <= CURRENT_DATE()
),
email_sends AS (
    SELECT 
        cd.CAMPAIGN_ID,
        cd.PRODUCT_ID,
        cd.CAMPAIGN_TYPE,
        h.HCP_ID,
        CASE WHEN UNIFORM(0, 1, RANDOM()) < 0.5 THEN 'A' ELSE 'B' END AS VARIANT,
        DATEADD('day', UNIFORM(0, 3, RANDOM()), cd.CAMPAIGN_START_DATE)::DATE AS SENT_DATE,
        -- Variant B has 15% higher open rate
        CASE 
            WHEN UNIFORM(0, 100, RANDOM()) < (CASE WHEN VARIANT = 'A' THEN 25 ELSE 29 END)
            THEN DATEADD('hour', UNIFORM(1, 72, RANDOM()), SENT_DATE)::TIMESTAMP
            ELSE NULL
        END AS OPEN_DATE,
        NULL::TIMESTAMP AS CLICK_DATE
    FROM campaign_dates cd
    CROSS JOIN (SELECT HCP_ID FROM HEALTHCARE_PROVIDERS ORDER BY RANDOM() LIMIT 2000) h
)
SELECT 
    CAMPAIGN_ID,
    PRODUCT_ID,
    CAMPAIGN_TYPE,
    HCP_ID,
    VARIANT,
    SENT_DATE,
    OPEN_DATE,
    -- Click only if opened, Variant B has 20% higher click rate given open
    CASE 
        WHEN OPEN_DATE IS NOT NULL AND UNIFORM(0, 100, RANDOM()) < (CASE WHEN VARIANT = 'A' THEN 35 ELSE 42 END)
        THEN DATEADD('minute', UNIFORM(1, 120, RANDOM()), OPEN_DATE)::TIMESTAMP
        ELSE NULL
    END AS CLICK_DATE
FROM email_sends;

-- =============================================================================
-- 7. CREATE VIEWS FOR ML SCHEMA
-- =============================================================================
USE SCHEMA ML;

-- Aggregated prescription features view
CREATE OR REPLACE VIEW PRESCRIPTION_FEATURES AS
SELECT 
    PRODUCT_ID,
    HCP_ID,
    REGION,
    FILL_DATE AS AS_OF_DATE,
    SUM(QUANTITY) OVER (
        PARTITION BY PRODUCT_ID, HCP_ID 
        ORDER BY FILL_DATE 
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS RX_VOLUME_2W,
    SUM(QUANTITY) OVER (
        PARTITION BY PRODUCT_ID, HCP_ID 
        ORDER BY FILL_DATE 
        ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
    ) AS RX_VOLUME_4W,
    AVG(QUANTITY) OVER (
        PARTITION BY PRODUCT_ID, HCP_ID 
        ORDER BY FILL_DATE 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS RX_AVG_LAST_WEEK,
    COUNT(*) OVER (
        PARTITION BY PRODUCT_ID, HCP_ID 
        ORDER BY FILL_DATE 
        ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
    ) AS RX_COUNT_4W
FROM RAW.PRESCRIPTIONS;

-- Training targets view
CREATE OR REPLACE VIEW PRESCRIPTION_TARGETS AS
SELECT 
    PRODUCT_ID,
    HCP_ID,
    FILL_DATE AS AS_OF_DATE,
    SUM(QUANTITY) OVER (
        PARTITION BY PRODUCT_ID, HCP_ID 
        ORDER BY FILL_DATE 
        ROWS BETWEEN CURRENT ROW AND 29 FOLLOWING
    ) AS NEXT_30_DAY_VOLUME
FROM RAW.PRESCRIPTIONS;

-- =============================================================================
-- 8. GRANT PERMISSIONS
-- =============================================================================
CREATE ROLE IF NOT EXISTS AZURITY_ML_ROLE;

GRANT USAGE ON DATABASE AZURITY_DEMO_DB TO ROLE AZURITY_ML_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE AZURITY_DEMO_DB TO ROLE AZURITY_ML_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA AZURITY_DEMO_DB.RAW TO ROLE AZURITY_ML_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA AZURITY_DEMO_DB.ML TO ROLE AZURITY_ML_ROLE;
GRANT CREATE TABLE ON SCHEMA AZURITY_DEMO_DB.ML TO ROLE AZURITY_ML_ROLE;
GRANT CREATE VIEW ON SCHEMA AZURITY_DEMO_DB.ML TO ROLE AZURITY_ML_ROLE;
GRANT CREATE MODEL ON SCHEMA AZURITY_DEMO_DB.ML TO ROLE AZURITY_ML_ROLE;
GRANT CREATE DYNAMIC TABLE ON SCHEMA AZURITY_DEMO_DB.ML TO ROLE AZURITY_ML_ROLE;
GRANT USAGE ON WAREHOUSE AZURITY_ML_WH TO ROLE AZURITY_ML_ROLE;
GRANT USAGE ON COMPUTE POOL AZURITY_ML_POOL TO ROLE AZURITY_ML_ROLE;
GRANT USAGE ON INTEGRATION AZURITY_PYPI_EAI TO ROLE AZURITY_ML_ROLE;

-- Grant role to current user
GRANT ROLE AZURITY_ML_ROLE TO USER ADMIN;

-- =============================================================================
-- 9. VERIFY SETUP
-- =============================================================================
SELECT 'Products' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM RAW.PRODUCTS
UNION ALL
SELECT 'Healthcare Providers', COUNT(*) FROM RAW.HEALTHCARE_PROVIDERS
UNION ALL
SELECT 'Prescriptions', COUNT(*) FROM RAW.PRESCRIPTIONS
UNION ALL
SELECT 'Email Campaigns', COUNT(*) FROM RAW.EMAIL_CAMPAIGNS;

-- Show compute pool status
SHOW COMPUTE POOLS LIKE 'AZURITY%';

SELECT '✓ Azurity Demo Setup Complete!' AS STATUS;

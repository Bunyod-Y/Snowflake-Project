-- Create a new schema for clean data
CREATE SCHEMA IF NOT EXISTS SALES_PROJECT_DB.CURATED_DATA;

SELECT
    transaction_id,
    TO_DATE(date) as transaction_date, -- Ensuring it is strictly a date
    UPPER(COALESCE(region, 'UNKNOWN')) as region_clean, -- Handle NULLs & Uppercase
    product,
    amount as net_amount,
    CAST(amount * 1.10 AS DECIMAL(10,2)) as gross_amount_with_tax -- Calculate Tax
FROM SALES_PROJECT_DB.RAW_DATA.SALES_RAW
WHERE amount > 0;



CREATE OR REPLACE TABLE SALES_PROJECT_DB.CURATED_DATA.SALES_CLEAN AS
SELECT
    transaction_id,
    TO_DATE(date) as transaction_date,
    UPPER(COALESCE(region, 'UNKNOWN')) as region_clean,
    product,
    amount as net_amount,
    CAST(amount * 1.10 AS DECIMAL(10,2)) as gross_amount_with_tax,
    CURRENT_TIMESTAMP() as ingestion_time -- Good practice: track when this row was cleaned
FROM SALES_PROJECT_DB.RAW_DATA.SALES_RAW
WHERE amount > 0;

SELECT * FROM SALES_PROJECT_DB.CURATED_DATA.SALES_CLEAN;



USE SCHEMA SALES_PROJECT_DB.RAW_DATA;
-- Create a stream on the raw table
CREATE OR REPLACE STREAM sales_raw_stream ON TABLE SALES_RAW;



CREATE OR REPLACE TASK curating_sales_task
    WAREHOUSE = 'COMPUTE_WH' -- Use your specific warehouse name
    SCHEDULE = '1 MINUTE' 
    WHEN SYSTEM$STREAM_HAS_DATA('sales_raw_stream') -- Only run if there is new data
AS
INSERT INTO SALES_PROJECT_DB.CURATED_DATA.SALES_CLEAN
    (transaction_id, transaction_date, region_clean, product, net_amount, gross_amount_with_tax, ingestion_time)
SELECT
    transaction_id,
    TO_DATE(date),
    UPPER(COALESCE(region, 'UNKNOWN')),
    product,
    amount,
    CAST(amount * 1.10 AS DECIMAL(10,2)),
    CURRENT_TIMESTAMP()
FROM SALES_PROJECT_DB.RAW_DATA.sales_raw_stream -- <--- Select from the STREAM
WHERE METADATA$ACTION = 'INSERT' -- Only process new inserts
AND amount > 0;

ALTER TASK curating_sales_task RESUME;

SHOW TASKS; -- Look for 'state' = 'STARTED'

COPY INTO SALES_PROJECT_DB.RAW_DATA.SALES_RAW
FROM @SALES_STAGE_GCP
FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
ON_ERROR = 'CONTINUE';

SELECT * FROM SALES_PROJECT_DB.CURATED_DATA.SALES_CLEAN ORDER BY ingestion_time DESC;
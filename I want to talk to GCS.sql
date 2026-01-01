-- Create a Storage Integration object
CREATE OR REPLACE STORAGE INTEGRATION gcp_sales_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://snowflake-project-landing-gcp/');

  DESC STORAGE INTEGRATION gcp_sales_int;
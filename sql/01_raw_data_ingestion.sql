--Create a storage integration object to connect to AWS
CREATE OR REPLACE storage integration s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '<AWS_IAM_ROLE_ARN>'
    STORAGE_ALLOWED_LOCATIONS = ('<S3_FOLDER_URL>');

-- Get IAM User ARN to grant access to S3 by updating AWS policy (I did this in
-- Roles - > Edit Trust Relationships in AWS console)
DESC storage integration s3_int;

-- Create file format object for stage
CREATE OR REPLACE file format MANAGE_DB.FILE_FORMATS.ff_csv
    type = CSV
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL', 'null')
    empty_field_as_null = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';


-- Creating a stage to pull data from S3
CREATE OR REPLACE STAGE MANAGE_DB.EXTERNAL_STAGES.aws_stage
    url = '<S3_FOLDER_URL>'
    file_format = MANAGE_DB.FILE_FORMATS.ff_csv
    storage_integration = s3_int

-- Double check that we can see all the files we granted access to
LIST @MANAGE_DB.EXTERNAL_STAGES.aws_stage;

-- Creating our database along with the schema for our raw data 
CREATE OR REPLACE DATABASE OLIST_ANALYTICS;

USE DATABASE OLIST_ANALYTICS;

CREATE OR REPLACE SCHEMA OLIST_ANALYTICS.RAW;

-- Orders Table
CREATE OR REPLACE TABLE OLIST_ANALYTICS.RAW.ORDERS (
    order_id STRING,
    customer_id STRING,
    order_status STRING,
    order_purchase_timestamp TIMESTAMP_NTZ,
    order_approved_at TIMESTAMP_NTZ,
    order_delivered_carrier_date TIMESTAMP_NTZ,
    order_delivered_customer_date TIMESTAMP_NTZ,
    order_estimated_delivery_date TIMESTAMP_NTZ
);

COPY INTO OLIST_ANALYTICS.RAW.ORDERS
FROM @MANAGE_DB.EXTERNAL_STAGES.aws_stage
files = ('olist_orders_dataset.csv')
-- VALIDATION_MODE = RETURN_10_ROWS;  

-- SELECT * FROM RAW.ORDERS WHERE ORDER_ID = '53cdb2fc8bc7dce0b6741e2150273451'

-- Order Items 
CREATE OR REPLACE TABLE OLIST_ANALYTICS.RAW.ORDER_ITEMS (
    order_id STRING,
    order_item_id STRING,
    product_id STRING,
    seller_id STRING,
    shipping_limit_date TIMESTAMP_NTZ,
    price DOUBLE,
    freight_value DOUBLE
);

COPY INTO OLIST_ANALYTICS.RAW.ORDER_ITEMS
FROM @MANAGE_DB.EXTERNAL_STAGES.aws_stage
files = ('olist_order_items_dataset.csv')
-- VALIDATION_MODE = RETURN_10_ROWS;  

-- Order Payments
CREATE OR REPLACE TABLE OLIST_ANALYTICS.RAW.ORDER_PAYMENTS (
    order_id STRING,
    payment_sequential INT,
    payment_type STRING,
    payment_installments INT,
    payment_value DOUBLE
);

-- TRUNCATE TABLE OLIST_ANALYTICS.RAW.ORDER_PAYMENTS

COPY INTO OLIST_ANALYTICS.RAW.ORDER_PAYMENTS
FROM @MANAGE_DB.EXTERNAL_STAGES.aws_stage
files = ('olist_order_payments_dataset.csv')
-- VALIDATION_MODE = RETURN_ERRORS
-- VALIDATION_MODE = RETURN_10_ROWS;  


-- Customers Table
CREATE OR REPLACE TABLE OLIST_ANALYTICS.RAW.CUSTOMERS (
    customer_id STRING,
    customer_unique_id STRING,
    customer_zip_code_prefix STRING,
    customer_city STRING,
    customer_state STRING
);

COPY INTO OLIST_ANALYTICS.RAW.CUSTOMERS
FROM @MANAGE_DB.EXTERNAL_STAGES.aws_stage
files = ('olist_customers_dataset.csv')
-- VALIDATION_MODE = RETURN_10_ROWS;  

-- Products Table
CREATE OR REPLACE TABLE OLIST_ANALYTICS.RAW.PRODUCTS (
    product_id STRING,
    product_category_name STRING,
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

COPY INTO OLIST_ANALYTICS.RAW.PRODUCTS
FROM @MANAGE_DB.EXTERNAL_STAGES.aws_stage
files = ('olist_products_dataset.csv')
-- VALIDATION_MODE = RETURN_10_ROWS;  

-- Product Category Name Translation
CREATE OR REPLACE TABLE OLIST_ANALYTICS.RAW.PRODUCT_CATEGORY_TRANSLATION (
    product_category_name STRING,
    product_category_name_english STRING
    );

COPY INTO OLIST_ANALYTICS.RAW.PRODUCT_CATEGORY_TRANSLATION
FROM @MANAGE_DB.EXTERNAL_STAGES.aws_stage
files = ('product_category_name_translation.csv')
-- VALIDATION_MODE = RETURN_ERRORS
-- VALIDATION_MODE = RETURN_10_ROWS;  
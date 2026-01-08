-- We can create an Analytics Schema to better define our data model using aggregated metrics and updated business logic
CREATE OR REPLACE SCHEMA OLIST_ANALYTICS.ANALYTICS;

-- For validation - Confirming assumption that some orders have multiple payments (installments)
-- SELECT COUNT(payment_value)
-- FROM OLIST_ANALYTICS.RAW.ORDER_PAYMENTS
-- GROUP BY order_id
-- HAVING COUNT(payment_value) > 1
-- LIMIT 5

-- Orders Fact Table
-- Adding aggregated columns for total item value and total payment value
-- This makes the grain one row per order_id, which will simplify analysis
CREATE OR REPLACE TABLE OLIST_ANALYTICS.ANALYTICS.FACT_ORDERS AS

WITH orders AS (
    SELECT 
        order_id,
        customer_id,
        CAST(order_purchase_timestamp AS DATE) AS order_date,
        order_status,
        order_purchase_timestamp, 
        order_delivered_customer_date
    FROM
        OLIST_ANALYTICS.RAW.ORDERS
),
order_items_agg AS (
    SELECT
        order_id, -- for join
        SUM(price) AS total_item_value
    FROM 
        OLIST_ANALYTICS.RAW.ORDER_ITEMS
    GROUP BY 
        order_id
),
payments_agg AS (
    SELECT 
        order_id, 
        SUM(payment_value) AS total_payment_value
    FROM 
        OLIST_ANALYTICS.RAW.ORDER_PAYMENTS
    GROUP BY
        order_id        
)
SELECT 
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_status,
    COALESCE(oi.total_item_value, 0) AS total_item_value,
    COALESCE(p.total_payment_value, 0) AS total_payment_value,
    CASE WHEN 
        o.order_status = 'delivered'  AND
        o.order_delivered_customer_date IS NOT NULL
    THEN DATEDIFF(
            DAY, 
            o.order_purchase_timestamp, 
            o.order_delivered_customer_date)
    ELSE
        NULL
    END AS delivery_time_days,
    CASE WHEN 
        o.order_status = 'delivered'
    THEN TRUE
    ELSE FALSE
    END AS is_delivered
FROM orders o
LEFT JOIN order_items_agg oi
ON o.order_id = oi.order_id
LEFT JOIN payments_agg p
ON o.order_id = p.order_id;

-- Customers Dimension Table
-- Adding a derived column for customer region based on state
CREATE OR REPLACE TABLE OLIST_ANALYTICS.ANALYTICS.DIM_CUSTOMERS AS
SELECT 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    CASE 
        WHEN customer_state IN ('SP','RJ','MG','ES') THEN 'Southeast'
        WHEN customer_state IN ('PR','SC','RS') THEN 'South'
        WHEN customer_state IN ('DF','GO','MT','MS') THEN 'Central-West'
        WHEN customer_state IN ('BA','SE','AL','PE','PB','RN','CE','PI','MA') THEN 'Northeast'
        WHEN customer_state IN ('AM','PA','RR','AP','TO','RO','AC') THEN 'North'
        ELSE 'Unknown'
    END AS customer_region
FROM 
    OLIST_ANALYTICS.RAW.CUSTOMERS;

-- Products Dimension Table
-- Adding a derived column for product volume in cubic centimeters
-- Joining with product category translation for English category names
CREATE OR REPLACE TABLE OLIST_ANALYTICS.ANALYTICS.DIM_PRODUCTS AS
SELECT
    p.product_id,
    p.product_category_name,
    pt.product_category_name_english,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    (p.product_length_cm * p.product_width_cm * p.product_height_cm) AS product_volume_cm3
FROM 
    OLIST_ANALYTICS.RAW.PRODUCTS p
LEFT JOIN
    OLIST_ANALYTICS.RAW.PRODUCT_CATEGORY_TRANSLATION pt
ON 
    p.product_category_name = pt.product_category_name;

-- Order Items Fact Table
-- Adding a derived column for total item value (price + freight_value)
CREATE OR REPLACE TABLE OLIST_ANALYTICS.ANALYTICS.FACT_ORDER_ITEMS AS
SELECT 
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    COALESCE(price, 0) + COALESCE(freight_value, 0) AS item_total_value
FROM
    OLIST_ANALYTICS.RAW.ORDER_ITEMS

-- Validation - Confirming assumption about freight value being per order_item and not compounded onto
-- total value
-- SELECT 
--     order_id, 
--     COUNT(order_item_id) AS num_order_items, 
--     SUM(price) AS order_total_without_shipping,
--     SUM(freight_value) AS total_shipping,
--     SUM(item_total_value) AS order_total_including_shipping
-- FROM OLIST_ANALYTICS.ANALYTICS.FACT_ORDER_ITEMS
-- GROUP BY order_id
-- HAVING COUNT(order_item_id) > 1
-- ORDER BY num_order_items DESC 
-- LIMIT 3
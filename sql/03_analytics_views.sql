-- Enriched Orders View
-- Grain of one row per order
-- Will allow for Order level analysis in PowerBI without the need for additional joins/modelling
CREATE OR REPLACE VIEW OLIST_ANALYTICS.ANALYTICS.VW_ORDERS_ENRICHED AS

SELECT 
    fo.order_id,
    fo.order_date,
    fo.order_status,
    fo.is_delivered,
    fo.delivery_time_days,
    fo.total_item_value,
    fo.total_payment_value,
    dc.customer_unique_id,
    dc.customer_region,
    dc.customer_state,
    dc.customer_city
FROM 
    OLIST_ANALYTICS.ANALYTICS.FACT_ORDERS fo
LEFT JOIN 
    OLIST_ANALYTICS.ANALYTICS.DIM_CUSTOMERS dc
ON 
    fo.customer_id = dc.customer_id;

-- Product Sales View
-- Grain of one row per product
-- Product level sales metrics derived from Order Item data (revenue, shipping, product performance)
CREATE OR REPLACE VIEW OLIST_ANALYTICS.ANALYTICS.VW_PRODUCT_SALES AS

SELECT 
    dp.product_id,
    dp.product_category_name_english,
    COUNT(DISTINCT foi.order_id) AS num_orders,
    SUM(foi.price) AS product_revenue,
    SUM(foi.freight_value) AS shipping_revenue,
    SUM(foi.item_total_value) AS total_revenue
    
FROM 
    OLIST_ANALYTICS.ANALYTICS.DIM_PRODUCTS dp
LEFT JOIN 
    OLIST_ANALYTICS.ANALYTICS.FACT_ORDER_ITEMS foi
ON 
    dp.product_id = foi.product_id
GROUP BY 
    dp.product_id, 
    dp.product_category_name_english;

-- Customer Metrics View
-- Grain of one row per customer
-- Exposes customer level behavior insights (lifetime value, avg order value, avg delivery time)
CREATE OR REPLACE VIEW OLIST_ANALYTICS.ANALYTICS.VW_CUSTOMER_METRICS AS 

SELECT 
    dc.customer_unique_id,
    dc.customer_region,
    dc.customer_state,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    AVG(fo.total_payment_value) AS avg_order_value,
    SUM(fo.total_payment_value) AS lifetime_value,
    AVG(fo.delivery_time_days) AS avg_delivery_time_days
FROM 
    OLIST_ANALYTICS.ANALYTICS.FACT_ORDERS fo
LEFT JOIN 
    OLIST_ANALYTICS.ANALYTICS.DIM_CUSTOMERS dc
ON 
    fo.customer_id = dc.customer_id
GROUP BY 
    dc.customer_unique_id,
    dc.customer_region,
    dc.customer_state;
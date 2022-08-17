-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Unity Catalog : Support for Identity Columns, Primary + Foreign Key Constraints
-- MAGIC 
-- MAGIC <img src="https://github.com/althrussell/databricks-demo/raw/main/product-demos/pkfk/images/erd.png" style="float:right; margin-left:10px" width="700"/>
-- MAGIC 
-- MAGIC As part of our efforts to make SQL great and support migrations from on-prem and alternative warehouse, we want to give customers convenient ways to build Entity Relationship Diagrams that are simple to maintain and evolve 
-- MAGIC 
-- MAGIC Value of the feature:
-- MAGIC - The ability to automatically generate auto-incrementing identify columns
-- MAGIC - Support for defining primary & foreign key constraints
-- MAGIC 
-- MAGIC Primary Key and Foreign Key are informational only and then won’t be enforced. 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Use case
-- MAGIC 
-- MAGIC Help the BI analyst to understand the entity relationships and how to join tables.
-- MAGIC 
-- MAGIC Requirements:
-- MAGIC - Unity Catalog enabled Workspace
-- MAGIC - DBR 11.1?
-- MAGIC - DBSQL Preview?
-- MAGIC - Hive Metastore is not supported
-- MAGIC 
-- MAGIC 
-- MAGIC <!-- tracking, please do not remove -->

-- COMMAND ----------

-- MAGIC %run ./_resources/00-init

-- COMMAND ----------

-- MAGIC %md ## 1/ Create a Dimension & Fact Tables In Unity Catalog
-- MAGIC 
-- MAGIC The first step is to create a Delta Tables in Unity Catalog.
-- MAGIC 
-- MAGIC We want to do that in SQL, to show multi-language support:
-- MAGIC 
-- MAGIC 1. Use the `CREATE TABLE` command and define a schema

-- COMMAND ----------

--TIME DIMENSION
CREATE OR REPLACE TABLE dim_time(
  time_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  SQL_date DATE,
  day_of_week INT,
  week_number INT
);
--STORE DIMENTION
CREATE OR REPLACE  TABLE dim_store(
  store_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  store_id STRING,
  store_name STRING,
  address STRING
);
--PRODUCT DIMENTION
CREATE OR REPLACE  TABLE dim_product(
  product_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sku STRING,
  description STRING,
  category STRING
);
--CUSTOMER DIMENTION
CREATE OR REPLACE  TABLE dim_customer(
  customer_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  customer_name STRING,
  customer_profile STRING,
  address STRING
);

CREATE OR REPLACE TABLE fact_sales(
  sales_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  time_key BIGINT NOT NULL CONSTRAINT dim_time_fk FOREIGN KEY REFERENCES dim_time,
  product_key BIGINT NOT NULL CONSTRAINT dim_product_fk FOREIGN KEY REFERENCES dim_product,
  store_key BIGINT NOT NULL CONSTRAINT dim_store_fk FOREIGN KEY REFERENCES dim_store,
  customer_key BIGINT NOT NULL CONSTRAINT dim_customer_fk FOREIGN KEY REFERENCES dim_customer,
  price_sold DOUBLE,
  units_sold INT,
  dollar_cost DOUBLE
);


-- COMMAND ----------

-- MAGIC %md ## 2/ Let's look at the table definition for DIM_TIME
-- MAGIC 
-- MAGIC The first step is to run DESCRIBE TABLE EXTENDED
-- MAGIC 
-- MAGIC Constraints are shown at the bottom of the results. 
-- MAGIC 
-- MAGIC - col_name : dim_time_pk 
-- MAGIC - data_type: PRIMARY KEY (`time_key`)

-- COMMAND ----------

DESCRIBE TABLE EXTENDED dim_time;

-- COMMAND ----------

-- MAGIC %md ## 3/ Let's add some data to the Dimension Tables
-- MAGIC 
-- MAGIC We want to do that in SQL, to show multi-language support:
-- MAGIC 1. Use the `INSERT INTO` command to insert some rows in the table

-- COMMAND ----------

INSERT INTO
  dim_time (SQL_date, day_of_week, week_number)
VALUES
  ('2010-01-01', 1, 1),
  ('2010-01-02', 2, 1),
  ('2010-01-03', 3, 1),
  ('2010-01-04', 4, 1),
  ('2010-01-05', 5, 1);
  
INSERT INTO
  dim_store (store_id, store_name, address)
VALUES
  ('0001', 'City Store', '1 Main Rd, Whoville');
  
INSERT INTO
  dim_product (sku, description, category)
VALUES
  ('1000001', 'High Tops', 'Ladies Shoes'),
  ('7000003', 'Printed T', 'Ladies Fashion Tops');
  
INSERT INTO
  dim_customer (customer_name, customer_profile, address)
VALUES
  (
    'REDACTED_NAME',
    'REDACTED_PROFILE',
    'REDACTED_ADDRESS'
  );

-- COMMAND ----------

-- MAGIC %md ## 4/ Let's add some data to the Fact Tables
-- MAGIC 
-- MAGIC We want to do that in SQL, to show multi-language support:
-- MAGIC 1. Use the `INSERT INTO` command to insert some rows in the table

-- COMMAND ----------

INSERT INTO
  fact_sales (
    time_key,
    product_key,
    store_key,
    customer_key,
    price_sold,
    units_sold,
    dollar_cost
  )
VALUES
  (1, 1, 1, 1, 100.99, 2, 2.99),
  (1, 2, 1, 1, 10.99, 2, 2.99),
  (3, 1, 1, 1, 100.99, 2, 2.99),
  (4, 1, 1, 1, 100.99, 2, 2.99),
  (4, 2, 1, 1, 10.99, 2, 2.99);

-- COMMAND ----------

-- MAGIC %md ## 5/ Primary Key and Foreign Key in Data Explorer
-- MAGIC 
-- MAGIC * TODO - Add Gif

-- COMMAND ----------

-- MAGIC %md ## 6/ Primary Key and Foreign Key in DBSQL - Code Completion
-- MAGIC 
-- MAGIC * TODO - Add Gif

-- COMMAND ----------

SELECT
  dim_product.*, fact_sales.price_sold, fact_sales.units_sold, fact_sales.dollar_cost,  dim_customer.*
FROM
  fact_sales
  INNER JOIN dim_product ON fact_sales.product_key = dim_product.product_key
  INNER JOIN dim_customer ON fact_sales.customer_key = dim_customer.customer_key;

-- COMMAND ----------

-- DBTITLE 1,Clean Up
-- MAGIC %python
-- MAGIC  spark.sql(f"DROP CATALOG {catalog} CASCADE")

-- COMMAND ----------



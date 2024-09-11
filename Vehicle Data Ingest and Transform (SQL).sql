-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Simplify ETL with Delta Live Table
-- MAGIC
-- MAGIC DLT makes Data Engineering accessible for all. Just declare your transformations in SQL or Python, and DLT will handle the Data Engineering complexity for you.
-- MAGIC
-- MAGIC <img style="float:right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-1.png" width="700"/>
-- MAGIC
-- MAGIC **Accelerate ETL development** <br/>
-- MAGIC Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
-- MAGIC
-- MAGIC **Remove operational complexity** <br/>
-- MAGIC By automating complex administrative tasks and gaining broader visibility into pipeline operations
-- MAGIC
-- MAGIC **Trust your data** <br/>
-- MAGIC With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
-- MAGIC
-- MAGIC **Simplify batch and streaming** <br/>
-- MAGIC With self-optimization and auto-scaling data pipelines for batch or streaming processing 
-- MAGIC
-- MAGIC ## Our Delta Live Table pipeline
-- MAGIC
-- MAGIC We'll be using as input a raw dataset containing information on our customers Loan and historical transactions. 
-- MAGIC
-- MAGIC Our goal is to ingest this data in near real time and build table for our Analyst team while ensuring data quality.
-- MAGIC
-- MAGIC **Your DLT Pipeline is ready!** Your pipeline was started using this notebook and is <a dbdemos-pipeline-id="dlt-loans" href="/#joblist/pipelines/b1b119d8-4a90-424d-b8c3-b1f4b14d9970">available here</a>.
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&aip=1&t=event&ec=dbdemos&ea=VIEW&dp=%2F_dbdemos%2Fdata-engineering%2Fdlt-loans%2F01-DLT-Loan-pipeline-SQL&cid=local&uid=local">

-- COMMAND ----------

-- MAGIC %md-sandbox 
-- MAGIC
-- MAGIC ## Bronze layer: incrementally ingest data leveraging Databricks Autoloader
-- MAGIC
-- MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-2.png" width="600"/>
-- MAGIC
-- MAGIC Our raw data is being sent to a blob storage. 
-- MAGIC
-- MAGIC Autoloader simplify this ingestion, including schema inference, schema evolution while being able to scale to millions of incoming files. 
-- MAGIC
-- MAGIC Autoloader is available in SQL using the `cloud_files` function and can be used with a variety of format (json, csv, avro...):
-- MAGIC
-- MAGIC For more detail on Autoloader, you can see `dbdemos.install('auto-loader')`
-- MAGIC
-- MAGIC #### STREAMING LIVE TABLE 
-- MAGIC Defining tables as `STREAMING` will guarantee that you only consume new incoming data. Without `STREAMING`, you will scan and ingest all the data available at once. See the [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-incremental-data.html) for more details

-- COMMAND ----------

CREATE STREAMING LIVE TABLE dlt_cgr_definitions_table_sql
AS SELECT * FROM cloud_files('/Volumes/beatrice_liew/vehicle_data/vehicle_data/cgr_definitions/','csv',map("cloudFiles.inferColumnTypes","true"))

-- COMMAND ----------

CREATE STREAMING LIVE TABLE dlt_cgr_premiums_table_sql
AS SELECT * FROM cloud_files('/Volumes/beatrice_liew/vehicle_data/vehicle_data/cgr_premiums/','csv',map("cloudFiles.inferColumnTypes","true"))

-- COMMAND ----------

CREATE STREAMING LIVE TABLE dlt_territory_definitions_table_sql
AS SELECT * FROM cloud_files('/Volumes/beatrice_liew/vehicle_data/vehicle_data/territory_definitions/','csv',map("cloudFiles.inferColumnTypes","true"))

-- COMMAND ----------

-- MAGIC %md-sandbox 
-- MAGIC
-- MAGIC ## Silver layer: joining tables while ensuring data quality
-- MAGIC
-- MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-3.png" width="600"/>
-- MAGIC
-- MAGIC Once the bronze layer is defined, we'll create the sliver layers by Joining data. Note that bronze tables are referenced using the `LIVE` spacename. 
-- MAGIC
-- MAGIC To consume only increment from the Bronze layer like `BZ_raw_txs`, we'll be using the `stream` keyworkd: `stream(LIVE.BZ_raw_txs)`
-- MAGIC
-- MAGIC Note that we don't have to worry about compactions, DLT handles that for us.
-- MAGIC
-- MAGIC #### Expectations
-- MAGIC By defining expectations (`CONSTRAINT <name> EXPECT <condition>`), you can enforce and track your data quality. See the [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html) for more details

-- COMMAND ----------

CREATE LIVE TABLE dlt_cgr_premiums_table_location_sql (
  constraint `valid age` EXPECT (customer_age <100 and customer_age > 0) on violation drop row,
  constraint `valid premium` EXPECT (current_premium > 0 and current_premium > fixed_expenses) on violation drop row
)
as select a.*, datediff(year, a.birthdate, CURRENT_DATE()) as customer_age, b.county, b.county_code, b.zipcode, b.town, b.area
from LIVE.dlt_cgr_premiums_table_sql a 
left join LIVE.dlt_territory_definitions_table_sql b
on a.territory = b.territory

-- COMMAND ----------

-- MAGIC %md-sandbox 
-- MAGIC
-- MAGIC ## Gold layer
-- MAGIC
-- MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-4.png" width="600"/>
-- MAGIC
-- MAGIC Our last step is to materialize the Gold Layer.
-- MAGIC
-- MAGIC Because these tables will be requested at scale using a SQL Endpoint, we'll add Zorder at the table level to ensure faster queries using `pipelines.autoOptimize.zOrderCols`, and DLT will handle the rest.

-- COMMAND ----------

CREATE LIVE TABLE  dlt_cgr_premiums_table_agg_sql 
as
select town, avg(current_premium) as average_premium, count(*) as number_of_customers
from LIVE.dlt_cgr_premiums_table_location_sql
group by town
order by average_premium desc


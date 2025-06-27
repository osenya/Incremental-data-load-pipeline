# Databricks notebook source
spark.sql('use silver_db')
spark.sql('show tables').show()
spark.sql('show databases').show()


# COMMAND ----------

 spark.sql("DROP TABLE IF EXISTS silver_customer")



# COMMAND ----------

# creating a table where we will upload our customer data
spark.sql('use silver_db')
spark.sql("""
CREATE TABLE if not exists silver_customer (
    Customer_id STRING,
    name STRING,
    email STRING,
    country STRING,
    customer_type STRING,
    registration_date DATE,
    age INT,
    gender STRING,
    total_purchases INT,
    customer_segment STRING,
    days_since_registration INT,
    last_updated TIMESTAMP
)
""")

spark.sql('describe table silver_customer').show()
 


# COMMAND ----------

# incremantal loading block of code

# getting the last processed time stamp
last_processed_df = spark.sql('select max(last_updated) as last_updated_time from silver_customer')
last_processed_row = last_processed_df.first()

last_updated_timestamp = last_processed_row['last_updated_time']

if last_updated_timestamp is None:
    last_updated_timestamp='1900-01-01T00:00:00.000+00:00'

# creating a temporary view of the incremental customers data

spark.sql(f"""
          create or replace temporary view bronze_incremental as 
          select * from globalretail_bronze.customer_table c 
          where c.Ingestion_time > '{last_updated_timestamp}'
          """)

# the c designates the alias for our table --->globalretail_bronze.customer_table
display(spark.sql('select * from bronze_incremental'))




# COMMAND ----------

# busines transformation requirements
# validate email address
# validate age between 18 and 30
# create customer segments as total purchases > 10000 then high value as if > 5000 then medium  value else low value
# days since a user is registered in the system
# remove any junk record where the total purchases is negative


# COMMAND ----------

# creating a temporary view that will hold the dataset that contains the dataset that meet the above objectives
spark.sql("""
          create or replace temporary view silver_incremental as select 
          customer_id,
          name,
        email,
        country,
        customer_type,
        registration_date,
        age,
        gender,
        total_purchases,
        CASE
            when total_purchases > 10000 then 'High_value'
            when total_purchases > 5000 then 'Medium_value'
            else 'low_value'
            end as customer_segment,
        datediff(current_date(),registration_date) as days_since_registration,
        current_timestamp() as last_updated
        from bronze_incremental

        where age between 18 and 30 
        and email is not null 
        and total_purchases>=0

          """)


# COMMAND ----------

display(spark.sql('select * from silver_incremental'))

# COMMAND ----------

# MAGIC %sql
# MAGIC         select age,
# MAGIC           count(age) as No_of_customers
# MAGIC           from silver_incremental
# MAGIC           group by age
# MAGIC           order by No_of_customers

# COMMAND ----------

# pushing our data from the silver_incremental to silver_customer using Merge
spark.sql("""
          merge into silver_customer as target
          using silver_incremental as source
          on target.customer_id = source.customer_id

        when matched then
           update set *
    
        when not matched then
              insert *

          """)

# COMMAND ----------

display(spark.sql('select count(*)  from silver_customer'))

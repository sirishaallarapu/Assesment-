# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("EmployeeDataProcessing").getOrCreate()


# COMMAND ----------

csv_path = "dbfs:/FileStore/tables/employees_data.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(csv_path)
df.show()

# COMMAND ----------


df_filtered = df.filter(df["salary"] > 55000)
df_filtered.show()


# COMMAND ----------

csv_path = "dbfs:/FileStore/tables/employees_data.csv"

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(csv_path)


# COMMAND ----------

df_delta = spark.read.format("delta").load("/mnt/delta/employees")
df_delta.show()


# COMMAND ----------

df_clean = df.dropna()
df_clean.show()

# COMMAND ----------

df_clean.write.format("delta").mode("overwrite").save("dbfs:/mnt/delta/employees_cleaned")

# COMMAND ----------

df_delta = spark.read.format("delta").load("dbfs:/mnt/delta/employees_cleaned")
df_delta.show()

# COMMAND ----------

print("Total Rows:", df_clean.count()) 


# COMMAND ----------

from pyspark.sql.functions import avg
df_avg_salary = df_clean.groupBy("department").agg(avg("salary").alias("avg_salary"))
df_avg_salary.show()


# COMMAND ----------


df_unique = df_clean.dropDuplicates(["email"])
df_unique.show()


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Lazy evaluation in Spark means that transformations on RDDs/DataFrames are not executed immediately. Instead, Spark builds a logical execution plan and waits until an action (like .show(), .count(), .collect()) is triggered to execute the computation.Here, filter() is lazy, and Spark doesn't execute it until .show() is called.

# COMMAND ----------

from pyspark.sql.functions import col

df_filtered = df.filter(col("salary") > 50000)
df_filtered.show()

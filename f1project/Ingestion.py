# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion from circuit.csv from data lake

# COMMAND ----------

# importing variables from user_variables

%store -r adls key
print(adls, key)


# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{adls}.dfs.core.windows.net",
    key)

# COMMAND ----------

from pyspark.sql.types import *
circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"abfss://rawdata@{adls}.dfs.core.windows.net/f1/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), \
    col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming the columns

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
                                        .withColumnRenamed("circuitRef", "circuit_ref") \
                                        .withColumnRenamed("lat", "latitude") \
                                        .withColumnRenamed("lng", "longitude") \
                                        .withColumnRenamed("alt", "altitude")


# COMMAND ----------

from pyspark.sql.functions import current_timestamp
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving CSV to Parquet after dropping one column and adding ingestion date

# COMMAND ----------

circuits_final_df.write.parquet(f"abfss://rawdata@{adls}.dfs.core.windows.net/f1/processed/", mode="overwrite")

# COMMAND ----------

df = spark.read.parquet(f"abfss://rawdata@{adls}.dfs.core.windows.net/f1/processed/")
display(df)
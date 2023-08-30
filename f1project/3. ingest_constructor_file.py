# Databricks notebook source
# MAGIC %store -r adls key
# MAGIC print(adls, key)

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{adls}.dfs.core.windows.net",
    key)

# COMMAND ----------

dbutils.fs.ls(f'abfss://rawdata@{adls}.dfs.core.windows.net/f1/')

# COMMAND ----------

spark.read.json(f"abfss://rawdata@{adls}.dfs.core.windows.net/f1/constructors.json").printSchema()

# COMMAND ----------

constructor_schema = "constructorId DOUBLE, constructorRef STRING, name STRING, nationality STRING, URL STRING"

# COMMAND ----------

df = spark.read \
.schema(constructor_schema) \
.json(f"abfss://rawdata@{adls}.dfs.core.windows.net/f1/constructors.json")
df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

constructor_df = df.select(col('constructorId').alias('constructor_id'), col('constructorRef').alias('constructor_ref'), col('name'), col('nationality'))
final_constructor_df = constructor_df.withColumn("ingestion_date", current_timestamp())
display(final_constructor_df)

# COMMAND ----------

final_constructor_df.write.parquet(f"abfss://rawdata@{adls}.dfs.core.windows.net/f1/processed/constructors", mode="overwrite")

# COMMAND ----------

display(dbutils.fs.ls(f"abfss://rawdata@{adls}.dfs.core.windows.net/f1/processed/constructors"))
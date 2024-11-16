# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
import pandas as pd

def load(dataset="dbfs:/FileStore/mini_project11/grad-students.csv"):
    # Initialize Spark session
    spark = SparkSession.builder.appName("grade_student").getOrCreate()

    # Read the dataset from DBFS
    spark_df = spark.read.csv(dataset, header=True, inferSchema=True)
    print(f"Dataset loaded from {dataset}")

    # Drop the Delta table if it exists, when I don't use "overwrite"
    spark.sql("DROP TABLE IF EXISTS grade_student_delta")

    # Write the Spark DataFrame to a Delta table
    spark_df.write.format("delta").mode("overwrite").saveAsTable("grade_student_delta")
    print("Data successfully written to Delta table 'grade_student_delta'")

    if spark.catalog.tableExists("grade_student_delta"):
      print("Table exists. Proceeding with overwrite.")
    else:
      print("Table does not exist. Creating a new one.")
      
    # Print the number of rows in the DataFrame
    nrows = spark_df.count()
    print(f"Number of rows in the dataset: {nrows}")



# COMMAND ----------

load()

# COMMAND ----------

def data_transform(table="grade_student_delta"):
    # Initialize Spark session
    spark = SparkSession.builder.appName("grade_student").getOrCreate()

    # Define STEM categories
    core_STEM = [
        'Engineering',
        'Computers & Mathematics',
        'Biology & Life Science',
        'Physical Sciences'
    ]

    other_STEM = [
        'Agriculture & Natural Resources',
        'Health',
        'Interdisciplinary'
    ]
    
    # Load the table into a DataFrame
    sparktable = spark.table(table)
    
    # Add the "STEM_major" column based on conditions
    sparktable = sparktable.withColumn(
        "STEM_major",
        when(col("Major_category").isin(core_STEM), "core_STEM")
        .when(col("Major_category").isin(other_STEM), "other_STEM")
        .otherwise("Other")
    )

    # Overwrite the Delta table with schema evolution
    sparktable.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(table)
    print(f"Table '{table}' updated successfully.")


# COMMAND ----------

data_transform()

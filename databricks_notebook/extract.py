# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import when, col
import pandas as pd

def extract_spark():
    url = "https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/college-majors/grad-students.csv"
    FILESTORE_PATH = "dbfs:/FileStore/mini_project11/"  # Use /tmp as an alternative
    dbfs_file_path = FILESTORE_PATH + "/grad-students.csv"
    
    # Initialize Spark session
    spark = SparkSession.builder.appName("grade_student").getOrCreate()
    # Fetch the data
    df = pd.read_csv(url)
    
    # Save the file locally in the cluster
    local_path = "/tmp/grad-students.csv"
    # Fetch data and save locally
    try:
        df = pd.read_csv(url)
        df.to_csv(local_path, index=False)
        print(f"CSV saved locally at {local_path}")
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None


    # Check for permissions on DBFS
    try:
        dbutils.fs.mkdirs(FILESTORE_PATH)
        dbutils.fs.cp(f"file:{local_path}", dbfs_file_path)
        print(f"CSV successfully saved to {dbfs_file_path}")
    except Exception as e:
        print(f"Error saving to DBFS: {e}")
        return None
    
    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(df)
    
    # To avoid mismatch between the schema of the existing Delta table and the DataFrame I am trying to append
    spark.sql("DROP TABLE IF EXISTS grade_student_delta")

    # Write to Delta table (no need for explicit directory creation)
    spark_df.write.format("delta").mode("append").saveAsTable("grade_student_delta")
    print("Data successfully written to Delta table 'grade_student_delta'")
    
    return dbfs_file_path



# COMMAND ----------

extract_spark()


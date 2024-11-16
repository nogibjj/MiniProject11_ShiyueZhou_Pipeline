# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
import pandas as pd

spark = SparkSession.builder.appName("grade_student").getOrCreate()

query_sample ="""
            SELECT 
                Major_category,
                SUM(Nongrad_employed) AS Total_Nongrad_employed,
                SUM(Grad_employed) AS Total_Grad_employed,
                SUM(Grad_unemployed) AS Total_Grad_unemployed,
                SUM(Nongrad_unemployed) AS Total_Nongrad_unemployed,
                SUM(Grad_total) AS Total_Grad_total,
                SUM(Nongrad_total) AS Total_Nongrad_total
            FROM grade_student_delta
            GROUP BY Major_category
            HAVING Total_Grad_employed + Total_Nongrad_employed > 10000
            ORDER BY Total_Grad_employed + Total_Nongrad_employed DESC
        """

query_result=spark.sql(query_sample)

query_result.show()

# COMMAND ----------

# Write a SQL query to the file
with open("sample_query.sql", "w") as file:
    file.write("SELECT * FROM grade_student_delta")

# Read the query from the file and execute it
with open("sample_query.sql", "r") as file:
    sql_query = file.read()

query_result = spark.sql(sql_query)
query_result.show()


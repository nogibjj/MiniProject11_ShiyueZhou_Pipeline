# from pyspark.sql import SparkSession
# from pyspark.sql.functions import when, col
# import pandas as pd

# def extract_spark():
#     url = "https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/college-majors/grad-students.csv"
#     FILESTORE_PATH = "dbfs:/FileStore/mini_project11/"  # Use /tmp as an alternative
#     dbfs_file_path = FILESTORE_PATH + "/grad-students.csv"
    
#     # Initialize Spark session
#     spark = SparkSession.builder.appName("grade_student").getOrCreate()
#     # Fetch the data
#     df = pd.read_csv(url)
    
#     # Save the file locally in the cluster
#     local_path = "/tmp/grad-students.csv"
#     df.to_csv(local_path, index=False)
#     print(f"CSV saved locally at {local_path}")
#     # Create the DBFS directory if it doesn't exist
#     dbutils.fs.mkdirs(FILESTORE_PATH)
#     # Copy the local file to DBFS
#     dbutils.fs.cp(f"file:{local_path}", dbfs_file_path)
#     print(f"CSV successfully saved to {dbfs_file_path}")
    
#     # Convert Pandas DataFrame to Spark DataFrame
#     spark_df = spark.createDataFrame(df)
    
#     # To avoid mismatch between the schema of the existing
#     # Delta table and the DataFrame I am trying to append
#     spark.sql("DROP TABLE IF EXISTS grade_student_delta")
#     # Write to Delta table (no need for explicit directory creation)
#     spark_df.write.format("delta").mode("append").saveAsTable("grade_student_delta")
#     print("Data successfully written to Delta table 'grade_student_delta'")
    
#     return dbfs_file_path


# def load(dataset="dbfs:/FileStore/mini_project11/grad-students.csv"):
#     # Initialize Spark session
#     spark = SparkSession.builder.appName("grade_student").getOrCreate()

#     # Read the dataset from DBFS
#     spark_df = spark.read.csv(dataset, header=True, inferSchema=True)
#     print(f"Dataset loaded from {dataset}")

#     # Drop the Delta table if it exists, when I don't use "overwrite"
#     spark.sql("DROP TABLE IF EXISTS grade_student_delta")

#     # Write the Spark DataFrame to a Delta table
#   spark_df.write.format("delta").mode("overwrite").saveAsTable("grade_student_delta")
#     print("Data successfully written to Delta table 'grade_student_delta'")

#     if spark.catalog.tableExists("grade_student_delta"):
#       print("Table exists. Proceeding with overwrite.")
#     else:
#       print("Table does not exist. Creating a new one.")
      
#     # Print the number of rows in the DataFrame
#     nrows = spark_df.count()
#     print(f"Number of rows in the dataset: {nrows}")

# def data_transform(table="grade_student_delta"):
#     # Initialize Spark session
#     spark = SparkSession.builder.appName("grade_student").getOrCreate()

#     # Define STEM categories
#     core_STEM = [
#         'Engineering',
#         'Computers & Mathematics',
#         'Biology & Life Science',
#         'Physical Sciences'
#     ]

#     other_STEM = [
#         'Agriculture & Natural Resources',
#         'Health',
#         'Interdisciplinary'
#     ]
    
#     # Load the table into a DataFrame
#     sparktable = spark.table(table)
    
#     # Add the "STEM_major" column based on conditions
#     sparktable = sparktable.withColumn(
#         "STEM_major",
#         when(col("Major_category").isin(core_STEM), "core_STEM")
#         .when(col("Major_category").isin(other_STEM), "other_STEM")
#         .otherwise("Other")
#     )

#     # Overwrite the Delta table with schema evolution
#     sparktable.write.format("delta") \
#         .mode("overwrite") \
#         .option("mergeSchema", "true") \
#         .saveAsTable(table)
#     print(f"Table '{table}' updated successfully.")


# if __name__ == "__main__":
#     extract_spark()
#     display(dbutils.fs.ls("dbfs:/FileStore/mini_project11/"))
#     load()
#     data_transform()
#     spark = SparkSession.builder.appName("grade_student").getOrCreate()

#     query_sample ="""
#             SELECT 
#                 Major_category,
#                 SUM(Nongrad_employed) AS Total_Nongrad_employed,
#                 SUM(Grad_employed) AS Total_Grad_employed,
#                 SUM(Grad_unemployed) AS Total_Grad_unemployed,
#                 SUM(Nongrad_unemployed) AS Total_Nongrad_unemployed,
#                 SUM(Grad_total) AS Total_Grad_total,
#                 SUM(Nongrad_total) AS Total_Nongrad_total
#             FROM grade_student_delta
#             GROUP BY Major_category
#             HAVING Total_Grad_employed + Total_Nongrad_employed > 10000
#             ORDER BY Total_Grad_employed + Total_Nongrad_employed DESC
#         """

#     query_result=spark.sql(query_sample)

#     query_result.show()
    
#     # Write a SQL query to the file
#     with open("sample_query.sql", "w") as file:
#         file.write("SELECT * FROM grade_student_delta")

#     # Read the query from the file and execute it
#     with open("sample_query.sql", "r") as file:
#         sql_query = file.read()

#     query_result = spark.sql(sql_query)
#     query_result.show()

    

import requests
from dotenv import load_dotenv
import os
# from pyspark.sql import SparkSession

# Load environment variables
load_dotenv()
DATABRICKS_API_KEY = os.getenv("DATABRICKS_API_KEY")
SERVER_HOSTNAME = os.getenv("SERVER_HOSTNAME")
HTTP_PATH = os.getenv("HTTP_PATH")

# Define URLs and file paths
url = "https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/college-majors/grad-students.csv"
FILESTORE_PATH = "dbfs:/FileStore/mini_project11/"
dbfs_file_path = FILESTORE_PATH + "grad-students.csv"
local_file_path = "/dbfs/FileStore/mini_project11/grad-students.csv"

def extract_spark():
    """
    Fetch data from the URL, save it locally, 
    load into a Spark DataFrame, and save as a Delta table.
    """
    # Step 1: Download the CSV file and save locally
    try:
        print(f"Downloading data from {url}")
        response = requests.get(url)
        response.raise_for_status()
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        with open(local_file_path, "wb") as file:
            file.write(response.content)
        print(f"File downloaded and saved locally at {local_file_path}")
    except Exception as e:
        print(f"Error fetching the data: {e}")
        return None
    
    # # Step 2: Initialize Spark session
    # try:
    #     spark = SparkSession.builder.appName("grade_student").getOrCreate()
    #     print("Spark session initialized.")
    # except Exception as e:
    #     print(f"Error initializing Spark session: {e}")
    #     return None

    # # Step 3: Load the CSV file into a Spark DataFrame
    # try:
    #     spark_df = spark.read.csv(dbfs_file_path, 
    #                header=True, inferSchema=True)
    #     print(f"Data successfully loaded into Spark 
    # DataFrame with {spark_df.count()} rows.")
    # except Exception as e:
    #     print(f"Error loading data into Spark DataFrame: {e}")
    #     return None

    # # Step 4: Save the DataFrame to a Delta table
    # try:
    #     spark_df.write.format("delta").
    #  mode("overwrite").saveAsTable("grade_student_delta")
    #     print("Data successfully written to Delta table 'grade_student_delta'.")
    # except Exception as e:
    #     print(f"Error writing data to Delta table: {e}")
    #     return None

    # # Step 5: Save the DataFrame as a CSV file
    # try:
    #     local_csv_path = "dbfs:/tmp/grad-students.csv"
    #     spark_df.write.csv(local_csv_path, mode="overwrite", header=True)
    #     print(f"Data successfully saved as CSV at {local_csv_path}")
    # except Exception as e:
    #     print(f"Error saving DataFrame as CSV: {e}")
    #     return None
    
    return dbfs_file_path


if __name__ == "__main__":
    dbfs_path = extract_spark()
    if dbfs_path:
        print(f"Pipeline completed successfully. Data stored at {dbfs_path}")
    else:
        print("Pipeline failed.")

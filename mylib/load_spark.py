
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import when, col
import pandas as pd
def extract_spark():
    url="https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/college-majors/grad-students.csv"
    file_path="data/grad-students.csv"
    spark = SparkSession.builder.appName("grade_student").getOrCreate() # start spark
    # spark_data = spark.read.csv(file_path) # load as csv
    df=pd.read_csv(url)
    spark_df = spark.createDataFrame(df) 
    # To avoid mismatch between the schema of the existing Delta table and the DataFrame I am trying to append
    spark.sql("DROP TABLE IF EXISTS grade_student_delta")
    spark_df.write.format("delta").mode("append").saveAsTable("grade_student_delta")
    
    return file_path

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
    sparktable = sparktable.withColumn(  # Use sparktable, not table
        "STEM_major",
        when(col("Major_category").isin(core_STEM), "core_STEM")
        .when(col("Major_category").isin(other_STEM), "other_STEM")
        .otherwise("Other")
    )

    # Display the transformed DataFrame
    sparktable.show()

    # Return the transformed DataFrame
    return sparktable.columns

def load_sql(table="grade_student_delta"):
    spark = SparkSession.builder.appName("grade_student").getOrCreate()
    
    try:
        # Run the SQL query
        query_result = spark.sql(f"""
            SELECT 
                Major_category,
                SUM(Nongrad_employed) AS Total_Nongrad_employed,
                SUM(Grad_employed) AS Total_Grad_employed,
                SUM(Grad_unemployed) AS Total_Grad_unemployed,
                SUM(Nongrad_unemployed) AS Total_Nongrad_unemployed,
                SUM(Grad_total) AS Total_Grad_total,
                SUM(Nongrad_total) AS Total_Nongrad_total
            FROM {table}
            GROUP BY Major_category
            HAVING Total_Grad_employed + Total_Nongrad_employed > 10000
            ORDER BY Total_Grad_employed + Total_Nongrad_employed DESC
        """)
        
        # Check if the result is empty
        if query_result.count() > 0:
            return query_result
        else:
            print("No results found for the query.")
            return None
    
    except Exception as e:
        print(f"Error executing SQL query: {e}")
        return None


if __name__ == "__main__":
    extract_spark()
    spark = SparkSession.builder.appName("grade_student").getOrCreate()
    spark.sql("SELECT * FROM grade_student_delta LIMIT 2").show()
    data_transform(table="grade_student_delta")
    query_result=load_sql(table="grade_student_delta")
    query_result.show()
    
    # with open("sample_query.sql", "r")as file:
    #     sql_query=file.read()
    #     query_result=spark.sql(sql_query)
    #     query_result.show()
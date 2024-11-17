from pyspark.sql import SparkSession

def run_query():
    # Initialize Spark session
    spark = SparkSession.builder.appName("grade_student_query").getOrCreate()

    query = """
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
    result = spark.sql(query)
    result.show()

if __name__ == "__main__":
    run_query()

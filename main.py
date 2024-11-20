"""
Main CLI or application entry point
"""
from mylib.extract_db import extract_spark
from mylib.load_transform import load
from mylib.query import run_query
import os

if __name__ == "__main__":
    # Print the current working directory
    current_directory = os.getcwd()
    print(f"Current working directory: {current_directory}")

    try:
        print("Starting pipeline...")

        # Step 1: Extract data
        print("Running data extraction...")
        extract_spark()
        print("Data extraction completed.")

        # Step 2: Load and transform data
        print("Running data loading and transformation...")
        load()
        print("Data loading and transformation completed.")

        # Step 3: Query data
        print("Running queries and generating results...")
        run_query()
        print("Queries completed.")

        print("Pipeline completed successfully!")

    except Exception as e:
        print(f"Pipeline failed: {e}")


# from mylib.extract import extract

# # from mylib.extract_db import extract_spark
# # from mylib.load_transform import load,data_transform
# # from mylib.query import run_query()


# if __name__ == "__main__":
#     # pylint: disable=no-value-for-parameter
#     # extract_spark()
#     # load()
#     # data_transform()
#     # run_query()
#     extract()

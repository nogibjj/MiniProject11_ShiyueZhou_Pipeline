"""
Main cli or app entry point
"""

from mylib.extract import extract

# from mylib.extract_db import extract_spark
# from mylib.load_transform import load,data_transform
# from mylib.query import run_query()


if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    # extract_spark()
    # load()
    # data_transform()
    # run_query()
    extract()

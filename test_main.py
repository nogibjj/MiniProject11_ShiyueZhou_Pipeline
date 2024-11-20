"""
Test for extract and Databricks DBFS functionality
"""

from mylib.extract import extract
import requests
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
DATABRICKS_API_KEY = os.getenv("DATABRICKS_API_KEY")
SERVER_HOSTNAME = os.getenv("SERVER_HOSTNAME")
HTTP_PATH = os.getenv("HTTP_PATH")

# Check if required environment variables are set
assert SERVER_HOSTNAME, "SERVER_HOSTNAME is not set in the environment."
assert DATABRICKS_API_KEY, "DATABRICKS_API_KEY is not set in the environment."

# File paths
FILESTORE_PATH = "dbfs:/FileStore/mini_project11/"
dbfs_file_path = FILESTORE_PATH + "grad-students.csv"
url = f"https://{SERVER_HOSTNAME}/api/2.0"


def test_extract():
    """
    Test the extract function from mylib.extract
    """
    expected_file_path = "data/grad-students.csv"
    result = extract()
    assert result == expected_file_path, f"Expected {expected_file_path}, got {result}"


def check_filestore_path(path, headers):
    """
    Check if the specified file path exists in Databricks DBFS
    """
    try:
        response = requests.get(url + f"/dbfs/get-status?path={path}", headers=headers)
        response.raise_for_status()
        return response.json().get("path") is not None
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return False
    except KeyError as e:
        print(f"Unexpected response format: {e}")
        return False


def test_databricks():
    """
    Test if the specified FILESTORE_PATH exists in Databricks DBFS
    """
    headers = {"Authorization": f"Bearer {DATABRICKS_API_KEY}"}
    assert (
        check_filestore_path(FILESTORE_PATH, headers) is True
    ), "Databricks file path test failed."


if __name__ == "__main__":
    # Run tests
    test_extract()
    test_databricks()

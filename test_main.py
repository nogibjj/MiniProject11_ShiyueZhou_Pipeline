"""
Test goes here

"""

from mylib.extract import extract


def test_extract():
    assert extract() == "data/grad-students.csv"

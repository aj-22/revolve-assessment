from pyspark.sql import SparkSession
import os

from solution_start import load_transactions_json, get_filepaths

def create_test_environment():
    """
    Creates a dummy file transactions.json at a dummy location
    :return:
    """
    location = r'./input_data/test/transactions/d=2018-01-01/'
    is_exist = os.path.exists(location)
    if not is_exist:
        os.makedirs(location)
    json_path = location+'transactions.json'
    json_string = '{"customer_id": "C9", "basket": [{"product_id": "P39", "price": 656}, {"product_id": "P37", "price": 1995}], "date_of_purchase": "2018-12-01 09:21:00"}'
    with open(json_path, "w") as jfile:
        jfile.write(json_string)


def test_load_transactions_json():
    """
    Function for testing test_load_transactions_json() function
    A test folder is created in input_data folder where a sample transactions.json file is placed
    The sample file contains one record as follows:
    {"customer_id": "C9", "basket": [{"product_id": "P39", "price": 656}, {"product_id": "P37", "price": 1995}], "date_of_purchase": "2018-12-01 09:21:00"}
    """
    location = r'./input_data/test/transactions/d=2018-01-01/'
    json_path = location+'transactions.json'

    spark = SparkSession.builder.appName("SparkApp").getOrCreate()

    output_df = load_transactions_json(spark,json_path)
    output_df.select("customer_id")
    expected_customers = ['C9']
    assert output_df.count() == 2, "Row count invalid"
    assert [row.customer_id for row in output_df.select("customer_id").distinct().collect()] == expected_customers, "Data invalid"
    print("Unit test for load_transactions_json() successful")


def test_get_filepaths():
    """
    Function for testing get_filepaths function
    Gets the test file path
    """
    location = "./input_data/test/transactions/"
    filepaths = get_filepaths(location)
    expected_filepaths1 = [r'./input_data/test/transactions\d=2018-01-01\transactions.json']
    expected_filepaths2 = [r'./input_data/test/transactions/d=2018-01-01/transactions.json']
    assert filepaths == expected_filepaths1 or filepaths == expected_filepaths2, "File path invalid"
    print("Unit Test for get_filepaths() successful")


if __name__ == '__main__':
    create_test_environment()
    test_get_filepaths()
    test_load_transactions_json()
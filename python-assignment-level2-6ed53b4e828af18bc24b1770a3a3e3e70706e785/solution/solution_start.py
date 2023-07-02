import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
import glob
import os


def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())


def load_transactions_json(spark: SparkSession, json_path: str) -> DataFrame:
    """
    :param spark: SparkSession object
    :param json_path: path where transactions.json file is stored
    :return: pyspark.sql.dataframe.DataFrame
    """
    #json_path = r'C:\docs\assessments\revolve\python-assignment-level2-6ed53b4e828af18bc24b1770a3a3e3e70706e785\input_data\starter\transactions\d=2018-12-01\transactions.json'
    schema = StructType([
        StructField("customer_id", StringType(),True),
        StructField("basket", ArrayType(StructType([
            StructField("product_id",StringType(), True),
            StructField("price",DoubleType(), True)
        ]))),
        StructField("date_of_purchase",TimestampType(),True)
    ])
    df = spark.read.schema(schema).json(json_path) \
        .select("customer_id",F.explode("basket").alias("basket"),"date_of_purchase") \
        .select("customer_id",F.col("basket.*"),"date_of_purchase")

    return df


def get_filepaths(location: str) -> [str]:
    """
    :param location: input folder location
    :return: List of filepaths
    """
    glob_list =  glob.glob(location+r'd=*/transactions.json')
    if glob_list == None:
        raise FileNotFoundError
    return glob_list


def save_df(df: DataFrame, location: str, filename: str):
    """
    :param df: spark dataframe
    :param location: output folder location
    :param filename: name of the output file
    :return: None
    Saves the dataframe in CSV format at the specified location
    """
    is_exist = os.path.exists(location)
    if not is_exist:
        os.makedirs(location)
    try:
        df.coalesce(1).write.mode("overwrite") \
            .options(header='True', delimiter=",") \
            .csv(location+r'/'+filename)
        print("Dataframe successfully saved")
    except Exception:
        print(Exception, "Unable to save")
        exit()


def main():
    # Get Parameters
    params = get_params()

    # Create Spark Session
    spark = SparkSession.builder.appName("SparkApp").getOrCreate()

    # Get input file locations of transactions
    try:
        transactions_list = get_filepaths(params['transactions_location'])
    except FileNotFoundError:
        print(FileNotFoundError, "Files not found")
        exit()

    # Load transactions and create dataframe
    df = load_transactions_json(spark, transactions_list[0])
    for filepath in transactions_list[1:]:
        df = df.union(load_transactions_json(spark, filepath))

    # Group the transactions data by customer_id and Product_id
    transactions_df = df.groupBy("customer_id","product_id").count()
    transactions_df = transactions_df.withColumnRenamed("count", "purchase_count")

    # load customers and products
    try:
        customers_df = spark.read.csv(params['customers_location'], header=True, inferSchema=True)
        products_df = spark.read.csv(params['products_location'], header=True, inferSchema=True)
    except Exception:
        print(Exception, "File input error")
        exit()

    # Joins : Inner join selected to ignore the missing data
    try:
        df = transactions_df.join(customers_df,on=transactions_df.customer_id==customers_df.customer_id,how='inner') \
            .select(transactions_df["customer_id"],transactions_df["product_id"],
                transactions_df["purchase_count"],customers_df["loyalty_score"])

        df = df.join(products_df, on=df.product_id==products_df.product_id, how="inner") \
            .select(df["customer_id"], df["loyalty_score"], df["product_id"],
                products_df["product_category"], df["purchase_count"])
    except Exception:
        print(Exception, "Joining failed")
        exit()
    # Save the dataframe in the output location
    save_df(df, params['output_location'], "output_data.csv")


if __name__ == "__main__":
    main()

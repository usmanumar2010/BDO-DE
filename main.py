import logging

from setup.spark_settings import setup_spark
from etl.extract import Extract
from etl.transform import Transform
from etl.load import  Load
APP_NAME = "DE App"

if __name__ == "__main__":
    input_path, output_path, spark = setup_spark(APP_NAME)
    logging.info("Application Done: %s", spark.sparkContext.appName)

    #extract data
    extract=Extract(input_path,spark)
    dataframe=extract.extract_data()

    #transform data
    transformer=Transform(dataframe,spark)
    avg_time_flyer_per_user,avg_time_on_each_flyer_per_user=transformer.apply_transformations()

    #load the data
    loader=Load(output_path)
    loader.load_data(avg_time_flyer_per_user,'avg_time_flyer_per_user')
    loader.load_data(avg_time_on_each_flyer_per_user,'avg_time_on_each_flyer_per_user')

    spark.stop()

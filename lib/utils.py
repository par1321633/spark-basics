import os
import configparser
from pyspark import SparkConf

def get_spark_app_config(job_name):
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read(os.path.join(os.getcwd(),'config','spark.conf'))

    for (key,value) in config.items(job_name):
        spark_conf.set(key, value)

    return spark_conf

def read_df(spark, filename=None):
    return spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv(filename)

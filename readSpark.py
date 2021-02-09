import sys
from pyspark.sql import *
from lib.utils import get_spark_app_config, read_df
import argparse

def get_cmdline_options():
    parser = argparse.ArgumentParser(description="Parse Command Line Args for Read Spark Program")
    parser.add_argument("-j", "--job-name", dest="job_name", required=True)
    parser.add_argument("-f", "--file-name", dest="file_name", required=True)
    args = parser.parse_args()
    return args

def main():
    print ("Starting Spark Read CSV Program")
    options = get_cmdline_options()
    conf = get_spark_app_config(options.job_name)
    print ("Create Spark Session")

    spark = SparkSession\
        .builder\
        .config(conf=conf)\
        .getOrCreate()

    conf_out = spark.sparkContext.getConf()
    #print(conf_out.toDebugString())

    #spark.read
    # This returns a DataFrameReader Object, This obj is the gateway to read the data in Apache.
    df = read_df(spark, filename=options.file_name)
    #print (df.show())

    #Repartitioning
    partitioned_df = df.repartition(2)

    #Transformations
    cnt_df = partitioned_df.where("Age < 40")\
        .select("Age","Gender","Country","state")\
        .groupBy("Country")\
        .count()


    print ("Length of Output", len(cnt_df.collect()))
    spark.stop()


if __name__ == '__main__':
    print ("Main Function Call")
    main()
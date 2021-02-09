from pyspark.sql import *
from lib.logger import Log4j

def main():
    spark = SparkSession\
        .builder\
        .appName("hello Spark")\
        .master("local[3]")\
        .getOrCreate()
    logger = Log4j(spark)
    print (logger)
    logger.info("Starting hello Spark Program")

    logger.info("Finished hello spark program")
    spark.stop()



if __name__ == '__main__':
    print ("Main Function Call")
    main()
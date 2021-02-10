"""
Logger Class
log4j is design to handle remote executors logging as well.
"""

class Log4j(object):
    def __init__(self, spark):
        print (spark)
        root_class = "pasharma.spark.learning.examples"
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        print (app_name)
        log4j = spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger(root_class + "." + app_name)

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)
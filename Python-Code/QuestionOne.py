from pyspark.sql import SparkSession
import logging
import configparser
logging.basicConfig(level=logging.INFO)


class Solution:

    def config_properties(self):

        config = configparser.ConfigParser()
        config.read("D:/Amadeus/resources/config.properties")
        return config

    def spark_session(self):
        logging.info("Creating the Spark Configuration")
        spark = SparkSession.builder.master("local[*]").appName("Amadeus").getOrCreate()
        return spark

    def read_bookings_file(self, config, sparksession):
        logging.info("Reading the bookings file")
        bookings = sparksession.read.format("csv").option("header", True) \
            .option("inferSchema", True).option("delimiter", "^").load(config.get("input", "booking_file"))

        logging.info('{}:{}'.format("Booking Count", bookings.count()))

    def read_search_file(self, config, sparksession):
        logging.info("Reading the searches file")
        searches = sparksession.getActiveSession().read.format("csv").option("header", True) \
            .option("inferSchema", True).option("delimiter", "^") \
            .load(config.get("input", "search_file"))
        logging.info('{}:{}'.format("Search Count", searches.count()))


amadeus = Solution()
config = amadeus.config_properties()
spark = amadeus.spark_session()
amadeus.read_bookings_file(config, spark)
amadeus.read_search_file(config, spark)

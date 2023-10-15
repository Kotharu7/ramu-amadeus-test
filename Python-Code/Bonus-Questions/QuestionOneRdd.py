from pyspark.sql import SparkSession
import logging
import configparser

logging.basicConfig(level=logging.INFO)


class Solution:

    def config_properties(self):
        config = configparser.ConfigParser()
        config.read("C:/Users/raman/Documents/GitHub/ramu-amadeus-test/resources/config.properties")
        return config

    def spark_session(self):
        logging.info("Creating the Spark Configuration")
        spark = SparkSession.builder.master("local[*]").appName("Amadeus").getOrCreate()
        sc = spark.sparkContext
        return sc

    def read_bookings_file(self, config, sc):
        logging.info("Reading the bookings file")
        bookings = sc.textFile(config.get("input", "booking_file"))
        header = bookings.first()
        bookings = bookings.filter(lambda row: row != header).distinct()
        logging.info('{}:{}'.format("Booking distinct Count", bookings.count()))

    def read_search_file(self, config, sc):
        logging.info("Reading the searches file")
        searches = sc.textFile(config.get("input", "search_file"))
        header = searches.first()
        searches = searches.filter(lambda row: row != header).distinct()
        logging.info('{}:{}'.format("Search distinct Count", searches.count()))


amadeus = Solution()
config = amadeus.config_properties()
sc = amadeus.spark_session()
amadeus.read_bookings_file(config, sc)
amadeus.read_search_file(config, sc)

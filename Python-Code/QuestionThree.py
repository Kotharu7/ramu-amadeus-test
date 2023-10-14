from pyspark.sql import SparkSession
import logging
from pyspark.sql import functions as F
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

    @staticmethod
    def read_bookings_file(config, sparksession):
        logging.info("Reading the bookings file")
        bookings = sparksession.read.format("csv").option("header", True) \
            .option("inferSchema", True).option("delimiter", "^").load(config.get("input", "booking_file"))

        return bookings

    @staticmethod
    def read_search_file(config, sparksession):
        logging.info("Reading the searches file")
        searches = sparksession.getActiveSession().read.format("csv").option("header", True) \
            .option("inferSchema", True).option("delimiter", "^") \
            .load(config.get("input", "search_file"))
        return searches

    def final_bookings(self, config, sparksession):
        searches = Solution.read_search_file(config, sparksession)
        bookings = Solution.read_bookings_file(config, sparksession)
        bookings.select("arr_port","dep_port")
        #logging.info('{}:{}'.format("bookings Count", bookings.select("arr_port","dep_port").count()))
        searches_with_bookings = searches. \
            join(bookings,
                 (F.trim(searches['Origin']) == F.trim(bookings['dep_port'])) & (F.trim(searches['Destination']) == F.trim(bookings[
                     'arr_port'])), "left").select(searches['*'],bookings.dep_port,bookings.arr_port).dropDuplicates()

        # searches_with_bookings.show(truncate=False)
        searches_with_bookings.printSchema()

        logging.info('{}:{}'.format("Search with bookings Count", searches_with_bookings.count()))


amadeus = Solution()
config = amadeus.config_properties()
spark = amadeus.spark_session()
amadeus.final_bookings(config, spark)

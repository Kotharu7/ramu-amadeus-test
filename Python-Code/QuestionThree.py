from pyspark.sql import SparkSession
import logging
from pyspark.sql import functions as F
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
        return spark

    @staticmethod
    def read_bookings_file(config, sparksession):
        logging.info("Reading the bookings file")
        bookings = sparksession.read.format("csv").option("header", True) \
            .option("inferSchema", True).option("delimiter", "^"). \
            load(config.get("input", "booking_file"))
        bookings = bookings.select("dep_port", "arr_port").dropDuplicates()

        return bookings

    @staticmethod
    def read_search_file(config, sparksession):
        logging.info("Reading the searches file")
        searches = sparksession.getActiveSession().read.format("csv").option("header", True) \
            .option("inferSchema", True).option("delimiter", "^") \
            .load(config.get("input", "search_file")).dropDuplicates()
        return searches

    def final_bookings(self, config, sparksession):
        searches = Solution.read_search_file(config, sparksession)
        bookings = Solution.read_bookings_file(config, sparksession)
        searches_with_bookings = searches.join(F.broadcast(bookings),
                                               (F.trim(searches['Origin']) == F.trim(bookings['dep_port'])) & (
                                                       F.trim(searches['Destination']) == F.trim(bookings['arr_port'])),
                                               "left")

        searches_with_bookings.printSchema()
        searches_with_bookings = searches_with_bookings. \
            withColumn("is_search_end_with_booking",
                       F.when(searches_with_bookings['arr_port'].isNotNull() & searches_with_bookings[
                           'dep_port'].isNotNull(), F.lit(1)).otherwise(F.lit(0))).drop("arr_port", "dep_port")

        searches_with_bookings.printSchema()
        searches_with_bookings.write.format("csv").mode(saveMode='overwrite').option("header", "true") \
            .save(config.get("output", "output_dir"))


amadeus = Solution()
config = amadeus.config_properties()
spark = amadeus.spark_session()
amadeus.final_bookings(config, spark)

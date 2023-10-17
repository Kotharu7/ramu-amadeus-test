from pyspark.sql import SparkSession
import logging
import configparser
from operator import add

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
        bookings_rdd = sc.textFile(config.get("input", "booking_file"))
        header = bookings_rdd.first()
        bookings = bookings_rdd.filter(lambda row: row != header).distinct()
        bookings = bookings.map(lambda row: row.split("^")).map(lambda row: (row[12].strip(), int(row[-4])))
        countPass = bookings.reduceByKey(add).takeOrdered(10, key=lambda x: -x[1])
        logging.info('{}:{}'.format("Booking distinct Count", countPass))


amadeus = Solution()
config = amadeus.config_properties()
sc = amadeus.spark_session()
amadeus.read_bookings_file(config, sc)

from pyspark.sql import SparkSession
import logging
import configparser
from operator import add

logging.basicConfig(level=logging.INFO)

"""  Implemented Bonus-2 question with Spark RDD."""

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
        ncol = len(header.split("^"))
        logging.info("{}:{}".format("Number of columns in file-->", ncol ))
        bookings = bookings_rdd.filter(lambda row: row != header).distinct()
        valid_bookings = bookings.filter(lambda x: (len(x.strip().split("^")) == ncol))
        logging.info("filter Airport and Passenger count field")
        air_port_pass = valid_bookings.map(lambda row: row.split("^")) \
            .map(lambda row: (row[12].strip(), int(row[-4])))
        logging.info("filter Airport and City fields")
        air_port_city = valid_bookings.map(lambda row: row.split("^")) \
            .map(lambda row: (row[12].strip(), row[13].strip())).distinct()
        logging.info("Counting the passengers by Airport ")
        pass_count_by_air_port = air_port_pass.reduceByKey(add)
        final_result = air_port_city.join(pass_count_by_air_port) \
            .sortBy(lambda x: x[1][1], False)
        logging.info("The output is in the order of Air port, City and Passenger count")
        logging.info('{}:{}'.format("Booking distinct Count", final_result.take(int(config.get("input", "top")))))


amadeus = Solution()
config = amadeus.config_properties()
sc = amadeus.spark_session()
amadeus.read_bookings_file(config, sc)

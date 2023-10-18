from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import logging
import configparser
from Utils.SchemaUtil import Schema
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

    def read_bookings_file(self, config, sparksession):
        logging.info("Reading the bookings file")
        bookings = sparksession.read.format("csv").schema(Schema.booking_schema()) \
            .option("mode", "PERMISSIVE") \
            .option("header", True).option("columnNameOfCorruptRecord", "_corrupt_record") \
            .option("delimiter", "^").load(config.get("input", "booking_file")) \
            .dropDuplicates().filter("year==2013 and _corrupt_record is null")
        logging.info('{}:{}'.format("Valid booking  record count", bookings.count()))
        logging.info("window specification")
        window_spec = Window.partitionBy("arr_port")
        bookings = bookings.withColumn("cnt_of_pax", F.sum(bookings['pax']).over(window_spec))
        top_bookings = bookings.select("*")
        logging.info("window specification to give the rank for top bookings")
        rank_window = Window.partitionBy("year").orderBy(top_bookings['cnt_of_pax'].desc())
        top_bookings = top_bookings.withColumn("rnk", F.dense_rank().over(rank_window)).filter("rnk <=10")
        top_bookings = top_bookings.select("arr_port", "arr_city", "cnt_of_pax", "rnk")\
            .dropDuplicates().orderBy(top_bookings['rnk'])
        top_bookings.show(50, truncate=False)


amadeus = Solution()
config = amadeus.config_properties()
spark = amadeus.spark_session()
amadeus.read_bookings_file(config, spark)
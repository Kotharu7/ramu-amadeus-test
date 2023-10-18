from pyspark.sql import SparkSession
import logging
import configparser
from Utils.SchemaUtil import Schema
from pyspark.sql.types import *
import pyspark.sql.functions as f

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
            .option("header", True).option("columnNameOfCorruptRecord", "_corrupt_record") \
            .option("delimiter", "^").load(config.get("input", "booking_file"))
        corrupted_records = bookings.where(bookings["_corrupt_record"].isNotNull())
        total_record_cnt = bookings.count()
        cleaned_recs = bookings.filter("_corrupt_record is null")
        duplicate_bookings = cleaned_recs.exceptAll(cleaned_recs.dropDuplicates(bookings.columns))
        duplicate_cnt = duplicate_bookings.count()
        valid_booking_cnt = cleaned_recs.distinct().count()
        logging.info('{}:{}'.format("Total  record count", total_record_cnt))
        logging.info('{}:{}'.format("Total duplicate record count", duplicate_cnt))
        logging.info('{}:{}'.format("Distinct booking count", valid_booking_cnt))
        logging.info('{}:{}'.format("corrupted_records  Count from the file",
                                    (total_record_cnt - duplicate_cnt - valid_booking_cnt)))

    def read_search_file(self, config, sparksession):
        logging.info("Reading the searches file")
        searches = sparksession.read.format("csv").schema(Schema.search_schema()) \
            .option("header", True) \
            .option("delimiter", "^").load(config.get("input", "search_file"))
        total_record_cnt = searches.count()
        corrupted_records = searches.filter("_corrupt_record is not null")
        valid_searches = searches.filter("_corrupt_record is null").drop("_corrupt_record")
        duplicate_searches = valid_searches.exceptAll(valid_searches.dropDuplicates(valid_searches.columns))
        duplicate_searches_cnt = duplicate_searches.count()
        valid_searches_cnt = valid_searches.distinct().count()
        logging.info('{}:{}'.format("Total record count", total_record_cnt))
        logging.info('{}:{}'.format("Total duplicate record count", duplicate_searches_cnt))
        logging.info('{}:{}'.format(" Total valid distinct  count", valid_searches_cnt))
        logging.info(
            '{}:{}'.format("corrupted records count", (total_record_cnt - duplicate_searches_cnt - valid_searches_cnt)))


amadeus = Solution()
config = amadeus.config_properties()
spark = amadeus.spark_session()
amadeus.read_bookings_file(config, spark)
amadeus.read_search_file(config, spark)
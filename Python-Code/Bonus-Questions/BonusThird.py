import mmap

from pyspark.sql import SparkSession
import logging
import configparser
import csv
logging.basicConfig(level=logging.INFO)


class Solution:

    def config_properties(self):
        config = configparser.ConfigParser()
        config.read("C:/Users/raman/Documents/GitHub/ramu-amadeus-test/resources/config.properties")
        return config

    def process_bookings(self, config):
        logging.info("Reading the bookings file")
        rec = set()
        with open(config.get("input", "booking_file")) as file_obj:
            reader_obj = csv.reader(file_obj, int(config.get("input","chunk_size")))
            next(reader_obj, None)
            for row in reader_obj:
                for x in row:
                    rec.add(x)

        logging.info('{}:{}'.format("distinct booking count", len(rec)))
        file_obj.close()

    def read_search_file(self, config):
        logging.info("Reading the searches file")
        rec = set()
        with open(config.get("input", "search_file")) as file_obj:
            reader_obj = csv.reader(file_obj, int(config.get("input","chunk_size")))
            next(reader_obj, None)
            for row in reader_obj:
                for x in row:
                    rec.add(x)
        logging.info('{}:{}'.format("distinct search count", len(rec)))
        file_obj.close()


amadeus = Solution()
config = amadeus.config_properties()
amadeus.process_bookings(config)
amadeus.read_search_file(config)

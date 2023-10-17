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

    @staticmethod
    def get_top_airports(rec):
        dict = {}

        for x in rec:
            try:
                if(len(x)>500):
                  key = x.split("^")[12].strip()
                  value = int(x.split("^")[-4])
                else:
                    key = x.split("^")[12].strip()
                    value = int(x.split("^")[-3])

                if dict.__contains__(key):
                    dict[key][len(dict.keys())] = value
                else:
                    dict[key] = {}
                    dict[key][0] = value
            except:
                print("invalid record--->", x)


        print(dict)

    def process_bookings(self, config):
        logging.info("Reading the bookings file")
        rec = set()
        with open(config.get("input", "booking_file")) as file_obj:
            reader_obj = csv.reader(file_obj, int(config.get("input", "chunk_size")))
            next(reader_obj, None)
            for row in reader_obj:
                for x in row:
                    if (len(x) > 200):
                        rec.add(x)

        #logging.info('{}:{}'.format("distinct booking count", len(rec)))
        file_obj.close()

        Solution.get_top_airports(list(rec))

    def read_search_file(self, config):
        logging.info("Reading the searches file")
        rec = set()
        with open(config.get("input", "search_file")) as file_obj:
            reader_obj = csv.reader(file_obj, int(config.get("input", "chunk_size")))
            next(reader_obj, None)
            for row in reader_obj:
                for x in row:
                    rec.add(x)
        logging.info('{}:{}'.format("distinct search count", len(rec)))
        file_obj.close()


amadeus = Solution()
config = amadeus.config_properties()
amadeus.process_bookings(config)
# amadeus.read_search_file(config)

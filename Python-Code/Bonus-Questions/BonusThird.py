import mmap
from pyspark.sql import SparkSession
import logging
import configparser
import csv
from collections import defaultdict
from operator import itemgetter
from heapq import nlargest

logging.basicConfig(level=logging.INFO)

""" Implemented bonus third question (hard): using Question: 2"""


class Solution:

    def config_properties(self):
        config = configparser.ConfigParser()
        config.read("C:/Users/raman/Documents/GitHub/ramu-amadeus-test/resources/config.properties")
        return config

    @staticmethod
    def top_airports(airport_pass, N, airport_city):
        result = {}
        for k, v in airport_pass.items():
            result[k] = sum(v)
        final_dict = dict(sorted(result.items(), key=lambda x: x[1], reverse=True)[:int(N)])
        for key in final_dict.keys():
            print("Airport-->",key, "  City--->", airport_city[key], "Count of Passengers-->",  final_dict[key])

    @staticmethod
    def get_airport_pass(rec, config):

        airport_pass = {}
        airport_city = {}

        for x in rec:
            try:
                if int(x.split("^")[-3]) == 2013:
                    lst = x.split("^")
                    airport_pass.setdefault(lst[12].strip(), []).append(int(lst[-4]))
                    airport_city[lst[12].strip()] = lst[13].strip()
            except:
                logging.exception('{}:{}'.format("exception due to the record",x))
        Solution.top_airports(airport_pass, config.get("input", "top"), airport_city)

    def process_bookings(self, config):
        logging.info("Reading the bookings file")
        valid_recs = set()
        corrupted_recs = set()
        with open(config.get("input", "booking_file")) as file_obj:
            logging.info("reading the header to count of columns in the file")
            reader = csv.reader(file_obj, delimiter="^")
            ncol = len(next(reader))
            logging.info('{}:{}'.format("Number of columns", ncol))
            logging.info("Reading the csv file in chunks, to improve the performance and memory efficient")
            reader_obj = csv.reader(file_obj, int(config.get("input", "chunk_size")))
            next(reader_obj, None)
            for row in reader_obj:
                for x in row:
                    if len(x.strip().split("^")) == ncol:
                        valid_recs.add(x)
                    else:
                        corrupted_recs.add(x)
        file_obj.close()
        Solution.get_airport_pass(list(valid_recs), config)


amadeus = Solution()
config = amadeus.config_properties()
amadeus.process_bookings(config)

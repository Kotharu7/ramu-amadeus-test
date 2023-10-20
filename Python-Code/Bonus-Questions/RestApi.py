import mmap
from pyspark.sql import SparkSession
import logging
import configparser
import csv
from collections import defaultdict
from operator import itemgetter
from heapq import nlargest
import flask
import json

logging.basicConfig(level=logging.INFO)

""" Implemented bonus 1st question """


class Solution:

    @staticmethod
    def config_properties():
        config = configparser.ConfigParser()
        config.read("C:/Users/raman/Documents/GitHub/ramu-amadeus-test/resources/config.properties")
        return config

    @staticmethod
    def top_airports(airport_pass, N, airport_city):
        result = {}
        top_airports = []
        for k, v in airport_pass.items():
            result[k] = sum(v)
        final_dict = dict(sorted(result.items(), key=lambda x: x[1], reverse=True)[:int(N)])

        for key in final_dict.keys():
            info = {
                "Airport": key,
                "City": airport_city[key],
                "Passenger-Count": final_dict[key]
            }
            top_airports.append(info)
        return top_airports

    @staticmethod
    def get_airport_pass(rec, config, N):

        airport_pass = {}
        airport_city = {}

        for x in rec:
            try:
                if int(x.split("^")[-3]) == 2013:
                    lst = x.split("^")
                    airport_pass.setdefault(lst[12].strip(), []).append(int(lst[-4]))
                    airport_city[lst[12].strip()] = lst[13].strip()
            except:
                logging.exception('{}:{}'.format("exception due to the record", x))
        return Solution.top_airports(airport_pass, N, airport_city)

    def read_bookings(self, N):
        config = Solution.config_properties()
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
        return Solution.get_airport_pass(list(valid_recs), config, N)

    def top_bookings(self, N):
        print("value of N", N)
        return Solution().read_bookings(N)

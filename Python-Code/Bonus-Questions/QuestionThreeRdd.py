from pyspark.sql import SparkSession
import logging
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
        sc = spark.sparkContext
        return sc

    @staticmethod
    def read_bookings_file(config, sc):
        logging.info("Reading the bookings file")
        bookings_rdd = sc.textFile(config.get("input", "booking_file"))
        header = bookings_rdd.first()
        bookings = bookings_rdd.filter(lambda row: row != header).distinct()
        bookings = bookings.map(lambda row: row.split("^")). \
            map(lambda row: (row[9].strip()+','+row[12].strip())).distinct()
        return bookings

    @staticmethod
    def read_search_file(config, sc):
        logging.info("Reading the searches file")
        searches = sc.textFile(config.get("input", "search_file"))
        header = searches.first()
        searches = searches.filter(lambda row: row != header).distinct().\
            map(lambda row: row.split("^")).map(lambda x:((x[0][5].strip()+','+x[0][6].strip()), x[0][1:4] + x[0][7:]))
        return searches

    def final_bookings(self, config, sc):
        #bookings = Solution.read_bookings_file(config, sc)
        searches = Solution.read_search_file(config, sc)
        print(searches.top(10))
        #print(bookings.top(5))
        # final_bookings= bookings.map(lambda x: (x[0], x[1])).rightOuterJoin(searches)
        #final_bookings = searches.join(bookings, searches[0][5].strip() == bookings[0] & searches[0][6].strip() == bookings[1], "left")
        # print("searches count-->", final_bookings.count())
        # print(final_bookings.top(5))


amadeus = Solution()
config = amadeus.config_properties()
sc = amadeus.spark_session()
amadeus.final_bookings(config, sc)

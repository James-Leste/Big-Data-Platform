import pandas as pd
from connector import initConnection, initDatabase
# from cassandra.query import BatchStatement
# from cassandra import ConsistencyLevel
# from cassandra.policies import FallthroughRetryPolicy
import logging
# from datetime import datetime

FILEPATH1 = "/Users/jamesroot/AAAroot/Course notes/Aalto Big Data Platforms/assignment-1-101699682/test/amazon_reviews_us_Digital_Software_v1_00.tsv"
FILEPATH2 = "/Users/jamesroot/AAAroot/Course notes/Aalto Big Data Platforms/assignment-1-101699682/test/amazon_reviews_us_Gift_Card_v1_00.tsv"
TESTFILEPATH = "/Users/jamesroot/AAAroot/Course notes/Aalto Big Data Platforms/assignment-1-101699682/test/test.tsv"

INSERT = "INSERT INTO reviews_by_id \
        (marketplace, customer_id, review_id, \
        product_id, product_parent, product_title, \
        product_category, star_rating, helpful_votes, \
        total_votes, verified_purchase, review_headline, \
        review_body, review_date) \
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"

logging.basicConfig(filename='data_ingestion_errors.log', level=logging.DEBUG, format='%(asctime)s:%(levelname)s:%(message)s')


def injesting(file, address, port, batchsize=3, delimiter="\t"):
    initDatabase("test", addresses=address, port=port)
    session = initConnection("test", addresses=address, port=port)
    insert_stmt = session.prepare(INSERT)
    reader = pd.read_csv(file, chunksize=batchsize, delimiter=delimiter)
    #batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM, retry_policy=FallthroughRetryPolicy())

    total = 0
    try:
        while True:
            df = next(reader)
            for i, s in df.iterrows():
                try:
                    session.execute_async(insert_stmt, [s.get("marketplace"), s.get("customer_id"), 
                                                s.get("review_id"), s.get("product_id"),
                                                s.get("product_parent"), s.get("product_title"), 
                                                s.get("product_category"), s.get("star_rating"), 
                                                s.get("helpful_votes"), s.get("total_votes"), 
                                                s.get("verified_purchase"), s.get("review_headline"), 
                                                s.get("review_body"), s.get("review_date")])
                    total += 1
                except AttributeError as e:
                    # Log the error and the problematic row for review
                    logging.error(f"Error processing row {i}: {e}")
                    logging.error(f"Problematic data: {s.to_dict()}")
                    logging.info("-----------------")
                    continue  # Skip the current row and continue with the next one
                except TypeError as e:
                    logging.error(f"Error processing row {i}: {e}")
                    logging.error(f"Problematic data: {s.to_dict()}")
                    logging.info("-----------------")
                    continue  # Skip the current row and continue with the next one
    except StopIteration:
        logging.info("Reached end of file.")
        logging.info("-----------------")
        print("Reached end of file.")
        print("-----------------")
    finally:
        logging.info(f"Total {total} records added successfully.")
        logging.info("-----------------")
        print(f"Total {total} records added successfully.")
        print("-----------------")
    session.shutdown()

def main():
    injesting(FILEPATH2, address=["35.204.192.40",], port=9042)
    

main()
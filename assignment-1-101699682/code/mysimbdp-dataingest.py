import pandas as pd
from connector import initConnection, initDatabase
# from cassandra.query import BatchStatement
# from cassandra import ConsistencyLevel
# from cassandra.policies import FallthroughRetryPolicy
import logging
from cassandra.query import UNSET_VALUE
import sys
# from datetime import datetime

FILEPATH1 = "/Users/jamesroot/AAAroot/Course notes/Aalto Big Data Platforms/assignment-1-101699682/test/amazon_reviews_us_Digital_Software_v1_00.tsv"
FILEPATH2 = "/Users/jamesroot/AAAroot/Course notes/Aalto Big Data Platforms/assignment-1-101699682/test/amazon_reviews_us_Gift_Card_v1_00.tsv"
TESTFILEPATH = "/Users/jamesroot/AAAroot/Course notes/Aalto Big Data Platforms/assignment-1-101699682/test/test.tsv"

INSERT = "INSERT INTO reviews_by_id \
        (marketplace, customer_id, review_id, \
        product_id, product_parent, product_title, \
        product_category, star_rating, helpful_votes, \
        total_votes, vine, verified_purchase, \
        review_headline, review_body, review_date) \
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"

logging.basicConfig(filename='data_ingestion_errors.log', level=logging.DEBUG, format='%(asctime)s:%(levelname)s:%(message)s')

def getColumnNames(filepath, delimiter='\t'):
    reader = pd.read_csv(filepath, chunksize=1, delimiter=delimiter)
    arr = next(reader).columns.array
    reader.close()
    return arr

def ingesting(filepath, address, port, batchsize=3, delimiter="\t"):
    initDatabase("test", addresses=address, port=port)
    session = initConnection("test", addresses=address, port=port)
    insert_stmt = session.prepare(INSERT)
    columns = getColumnNames(filepath)
    reader = pd.read_csv(filepath, chunksize=batchsize, delimiter=delimiter)
    #batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM, retry_policy=FallthroughRetryPolicy())
    total = 0
    try:
        while True:
            df = next(reader)
            for i, s in df.iterrows():
                try:
                    paraList = []
                    for n in columns:
                        paraList.append(UNSET_VALUE) if str(s.get(n)) == "nan"\
                            else paraList.append(s.get(n))
                        
                    session.execute_async(insert_stmt, paraList)
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
    if(sys.argv[1] == "-f"):
        ingesting(sys.argv[2], address=["34.32.197.190",], port=9042)
    else:
        print("Command Not Found, please use -f flag with file name")
    #print(getColumnNames(FILEPATH1))
    

if __name__ == "__main__":
    main()
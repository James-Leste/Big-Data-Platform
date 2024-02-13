import pandas as pd
import numpy as np
from connector import initConnection, initDatabase
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from cassandra.policies import FallthroughRetryPolicy

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

def injesting(file, batchsize=3, delimiter="\t"):
    initDatabase("test")
    session = initConnection("test")
    insert_stmt = session.prepare(INSERT)
    reader = pd.read_csv(file, chunksize=batchsize, delimiter=delimiter)
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM, retry_policy=FallthroughRetryPolicy())

    try:
        while True:
            df = next(reader)
            for i, s in df.iterrows():
                batch.add(insert_stmt, [s.get("marketplace"), s.get("customer_id"), 
                                              s.get("review_id"), s.get("product_id"),
                                              s.get("product_parent"), s.get("product_title"), 
                                              s.get("product_category"), s.get("star_rating"), 
                                              s.get("helpful_votes"), s.get("total_votes"), 
                                              s.get("verified_purchase"), s.get("review_headline"), 
                                              s.get("review_body"), s.get("review_date")])
                
                #print("Batch added: " + str(realbatchsize))
            session.execute(batch)
            #print("Batch written: " + str(realbatchsize))
            #print("------")
            batch.clear()
    except StopIteration:
        #print("total " + str(total) + " records added.")
        print("Reached end of file.")
    
    session.shutdown()

def main():
    injesting(FILEPATH2)
    

main()
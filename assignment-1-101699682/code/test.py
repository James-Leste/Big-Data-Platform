import pandas as pd
from connector import initConnection, initDatabase
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

import sys

# [s.get("marketplace"), s.get("customer_id"), 
# s.get("review_id"), s.get("product_id"),
# s.get("product_parent"), s.get("product_title"), 
# s.get("product_category"), s.get("star_rating"), 
# s.get("helpful_votes"), s.get("total_votes"), 
# s.get("verified_purchase"), s.get("review_headline"), 
# s.get("review_body"), s.get("review_date")]

FILEPATH1 = "/Users/jamesroot/AAAroot/Course notes/Aalto Big Data Platforms/assignment-1-101699682/test/test.tsv"
FILEPATH2 = "/Users/jamesroot/AAAroot/Course notes/Aalto Big Data Platforms/assignment-1-101699682/test/amazon_reviews_us_Gift_Card_v1_00.tsv"
INSERT = "INSERT INTO reviews_by_id \
        (marketplace, customer_id, review_id, \
        product_id, product_parent, product_title, \
        product_category, star_rating, helpful_votes, \
        total_votes, verified_purchase, review_headline, \
        review_body, review_date) \
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"


def main():
    #initDatabase("test")
    #session = initConnection("test")
    #insert_stmt = session.prepare(INSERT)
    reader = pd.read_csv(FILEPATH1, chunksize=10, delimiter="\t")
    
    #batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    realbatchsize = 0
    total = 0
    try:
        while True:
            df = next(reader)
            print("df: "+ str(df.shape))
            for i, s in df.iterrows():
                print("s: "+ s.get("review_body"))
                # batch.add(insert_stmt, [s.get("marketplace"), s.get("customer_id"), 
                #                               s.get("review_id"), s.get("product_id"),
                #                               s.get("product_parent"), s.get("product_title"), 
                #                               s.get("product_category"), s.get("star_rating"), 
                #                               s.get("helpful_votes"), s.get("total_votes"), 
                #                               s.get("verified_purchase"), s.get("review_headline"), 
                #                               s.get("review_body"), s.get("review_date")])
                realbatchsize = df.size
                total += realbatchsize
                #print("Batch added: " + str(realbatchsize))
            # session.execute(batch)
            #print("Batch written: " + str(realbatchsize))
            print("------")
            # batch.clear()
    except StopIteration:
        print("total " + str(total) + " records added.")
        print("Reached end of file.")


# reader = pd.read_csv(FILEPATH1, chunksize=1, delimiter="\t")
# df = next(reader)
# for i, s in df.iterrows():
#     print("marketplace: "+str(type(s.get("marketplace"))))
#     print("customer_id: "+str(type(s.get("customer_id"))))
#     print("product_id: "+str(type(s.get("product_id"))))
#     print("product_parent: "+str(type(s.get("product_parent"))))
#     print("product_title: "+str(type(s.get("product_title"))))
#     print("product_category: "+str(type(s.get("product_category"))))
#     print("star_rating: "+str(type(s.get("star_rating"))))
#     print("helpful_votes: "+str(type(s.get("helpful_votes"))))
#     print("total_votes: "+str(type(s.get("total_votes"))))
#     print("verified_purchase: "+str(type(s.get("verified_purchase"))))
#     print("review_headline: "+str(type(s.get("review_headline"))))
#     print("review_body:"+str(type(s.get("review_body"))))
#     print("review_date:"+str(type(s.get("review_date"))))
#     print(s.size)

# df = pd.read_csv(FILEPATH2, delimiter="\t")
# print(df.loc[4029])
# session = initConnection("test", addresses=["35.204.192.40"])
# result = session.execute("SELECT * FROM product;")
# for r in result:
#     print(r)
# df = pd.read_csv(FILEPATH2, delimiter="\t")
# print(type(df.iloc[12334].get("vine")))
def test():
    if(sys.argv[1] == "-f"):
        print(sys.argv[2])
    else:
        print("Command Not Found, please use -f flag with file name")

test()
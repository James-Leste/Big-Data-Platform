from cassandra.cluster import Cluster
ADDRESSES = "127.0.0.1"
PORT = 9042
def initConnection(keyspace, addresses=[ADDRESSES,], port=9042):
    cluster = Cluster(addresses, port=port)
    session = cluster.connect(keyspace=keyspace)
    print("Connecting...")
    print("Connected successfully !")
    return session

def initDatabase(keyspace="amazon"):
    session = initConnection(keyspace)
    session.execute("CREATE TABLE IF NOT EXISTS reviews_by_id (\
        marketplace text,\
        customer_id int,\
        review_id text,\
        product_id text,\
        product_parent text,\
        product_title text,\
        product_category text, \
        star_rating int,\
        helpful_votes int,\
        total_votes int,\
        verified_purchase text,\
        review_headline text,\
        review_body text,\
        review_date text,\
        PRIMARY KEY (review_id)\
    );")
    print("creating database table [reviews]")
    session.shutdown()

def main():
    pass
    
if __name__ == "__main__":
    main()
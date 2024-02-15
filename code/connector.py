from cassandra.cluster import Cluster
ADDRESSES = "127.0.0.1"
PORT = 9042
def initConnection(keyspace, addresses=[ADDRESSES,], port=9042):
    cluster = Cluster(addresses, port=port)
    session = cluster.connect(keyspace=keyspace)
    print("Connecting...")
    print("Connected successfully !")
    return session


# marketplace: <class 'str'>
# customer_id: <class 'int'>
# product_id: <class 'str'>
# product_parent: <class 'int'>
# product_title: <class 'str'>
# product_category: <class 'str'>
# star_rating: <class 'int'>
# helpful_votes: <class 'int'>
# total_votes: <class 'int'>
# vine: <class 'int'>
# verified_purchase: <class 'str'>
# review_headline: <class 'str'>
# review_body:<class 'str'>
# review_date:<class 'str'>
def initDatabase(keyspace="amazon", addresses=ADDRESSES, port=PORT):
    session = initConnection(keyspace, addresses, port=port)
    session.execute("CREATE TABLE IF NOT EXISTS reviews_by_id (\
        marketplace text,\
        customer_id int,\
        review_id text,\
        product_id text,\
        product_parent int,\
        product_title text,\
        product_category text, \
        star_rating int,\
        helpful_votes int,\
        total_votes int,\
        vine text,\
        verified_purchase text,\
        review_headline text,\
        review_body text,\
        review_date date,\
        PRIMARY KEY (review_id)\
    );")
    print("created database table [reviews] at keyspace:[" + keyspace + "]!")
    session.shutdown()
    return keyspace

def main():
    pass
    
if __name__ == "__main__":
    main()
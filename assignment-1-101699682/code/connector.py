from cassandra.cluster import Cluster
import os
ADDRESS = os.getenv('CASSANDRA_ADDRESS', '127.0.0.1')
PORT = int(os.getenv('CASSANDRA_PORT', 9042))

def createKeyspace(session, keyspace):
    try:
        session.execute("CREATE KEYSPACE IF NOT EXISTS "+ str(keyspace) +" \
            WITH REPLICATION = {\
            'class' : 'NetworkTopologyStrategy',\
            'DC1' : 2, \
            'DC2' : 1\
            };")
        print("Created keyspace: " + str(keyspace))
        return keyspace
    except Exception as e:
        print(f"Failed to create keyspace {keyspace}: {e}")
        raise

def initConnection(keyspace, addresses=[ADDRESS,], port=9042):
    try:
        cluster = Cluster(addresses, port=port)
        print(f"Connecting to {addresses}:{str(port)}")
        session = cluster.connect()
        createKeyspace(session, keyspace)
        session.set_keyspace(keyspace)
        print("Connected successfully !")
    except Exception as e:
        print(f"Failed to initialize connection: {e}")
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
def initDatabase(keyspace, addresses=ADDRESS, port=PORT):
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
    print(initConnection(keyspace="test"))
    
if __name__ == "__main__":
    main()
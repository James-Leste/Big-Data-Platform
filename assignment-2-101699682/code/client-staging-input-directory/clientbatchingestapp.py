import argparse
import json
from cassandra.cluster import Cluster
import logging
import pandas as pd
from cassandra.query import UNSET_VALUE
import datetime

INSERT = "INSERT INTO reviews_by_id \
        (marketplace, customer_id, review_id, \
        product_id, product_parent, product_title, \
        product_category, star_rating, helpful_votes, \
        total_votes, vine, verified_purchase, \
        review_headline, review_body, review_date) \
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"

def parse_params():
    parser = argparse.ArgumentParser(description='Read JSON configuration file.')
    parser.add_argument('-f','--file_path', type=str, help='Path to the JSON configuration file')
    parser.add_argument("--address", default='127.0.0.1', type=str, help="IP address of a Cassandra Node")
    parser.add_argument("--port", default=9042, type=int, help="Port number")

    args = parser.parse_args()
    with open(f"{args.file_path}/config.json", 'r') as file:
        config_data = json.load(file)

    # Now config_data contains the data from the specified config.json file
    return {
        "path": args.file_path, 
        "address": args.address, 
        "port": args.port
    }

def parse_config(client):
     # Use the file_path argument to open and read the JSON file
    with open(f"{client}/config.json", 'r') as file:
        config_data = json.load(file)
    return config_data

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

def initConnection(keyspace, addresses=['127.0.0.1',], port=9042):
    try:
        cluster = Cluster(addresses, port=port)
        print(f"Connecting to {addresses}:{str(port)}")
        session = cluster.connect()
        createKeyspace(session, keyspace)
        session.set_keyspace(keyspace)
        print("Connected successfully !")
        return session
    except Exception as e:
        print(f"Failed to initialize connection: {e}")
    

'''
marketplace: <class 'str'>
customer_id: <class 'int'>
product_id: <class 'str'>
product_parent: <class 'int'>
product_title: <class 'str'>
product_category: <class 'str'>
star_rating: <class 'int'>
helpful_votes: <class 'int'>
total_votes: <class 'int'>
vine: <class 'int'>
verified_purchase: <class 'str'>
review_headline: <class 'str'>
review_body:<class 'str'>
review_date:<class 'str'>
'''
def initDatabase(keyspace, address='127.0.0.1', port=9042):
    session = initConnection(keyspace, [address,], port=port)
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


def getColumnNames(filepath, delimiter='\t'):
    reader = pd.read_csv(filepath, chunksize=1, delimiter=delimiter)
    arr = next(reader).columns.array
    reader.close()
    return arr

def ingesting(filepath, address, port, batchsize=3, delimiter="\t"):
    with open(filepath, 'r') as file:
        total_lines = sum(1 for line in file)
    session = initConnection("test", addresses=[address,], port=port)
    insert_stmt = session.prepare(INSERT)
    columns = getColumnNames(filepath)
    reader = pd.read_csv(filepath, chunksize=batchsize, delimiter=delimiter)
    #batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM, retry_policy=FallthroughRetryPolicy())
    total = 0
    failed = 0
    start_time = datetime.datetime.now()
    print(f"Ingestion started at {start_time}")
    try:
        while True:
            df = next(reader)
            for i, s in df.iterrows():
                try:
                    paraList = []
                    for n in columns:
                        paraList.append(UNSET_VALUE) if str(s.get(n)) == "nan"\
                            else paraList.append(s.get(n))
                    future = session.execute_async(insert_stmt, paraList)
                    try:
                        results = future.result()
                        total += 1
                    except Exception:
                        failed += 1
                        print("Operation failed:")
                    finally:
                        print(f'Success:{total}; Failed: {failed}')
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
        end_time = datetime.datetime.now()
        ingestion_time = end_time - start_time
        print(f"Total ingestion time: {ingestion_time}")
        logging.info(f"Total {total} records added successfully.")
        logging.info("-----------------")
        print(f"Total {total} records added successfully.")
        print("-----------------")
    session.shutdown()

def main(): 
    args = parse_params()
    clientname = args.get("path")
    address = args.get("address")
    port = args.get("port")
    print(address, port)

    initDatabase('test', address, port)
    ingesting(f'{clientname}/test.tsv', address, port, batchsize=3, delimiter="\t")

if __name__ == "__main__":
    main()

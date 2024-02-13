from cassandra.cluster import Cluster
ADDRESSES = "127.0.0.1"
PORT = 9042
def initConnection(keyspace, addresses=[ADDRESSES,], port=9042):
    cluster = Cluster(addresses, port=port)
    session = cluster.connect(keyspace=keyspace)
    print("Connecting...")
    print("Connected successfully to " + session + "!")
    return session
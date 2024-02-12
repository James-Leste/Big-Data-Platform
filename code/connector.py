from cassandra.cluster import Cluster
def initConnection(addresses, port):
    cluster = Cluster(addresses, port=port)
    session = cluster.connect()
    return session

def main():
    addresses = ["127.0.0.1"]
    port1 = 9042
    port2 = 9043
    port3 = 9044
    db1session = initConnection(addresses, port1)
    db2session = initConnection(addresses, port2)
    db3session = initConnection(addresses, port3)
    print("connection success!")
    print(db1session)
    print(db2session)
    print(db3session)

main()
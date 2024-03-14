import sys
from cassandra.query import SimpleStatement
from cassandra.query import dict_factory

sys.path.insert(1, '../mysimbdp-coredms')

import connector

def getAll(keyspace, table):
    session = connector.initConnection(keyspace)
    session.row_factory = dict_factory
    rows = session.execute(f"SELECT * FROM {table};")
    session.shutdown()
    json = [dict(row) for row in rows]
    print(type(json[0].get("review_date")))
    return json

def main():
    getAll("test", "reviews_by_id")
    
if __name__ == "__main__":
    main()
from flask import Flask, jsonify
from cassandra.cluster import Cluster
#import sys
import dbservice
from markupsafe import escape

#sys.path.insert(1, '../mysimbdp-coredms')


app = Flask(__name__)

@app.route("/all/<keyspace>/<table>")
def getAllData(keyspace, table):
    rows = dbservice.getAll(keyspace, table)
    return (jsonify(rows), 200)
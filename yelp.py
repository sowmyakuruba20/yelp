from flask import Flask, jsonify
from neo4j import GraphDatabase

app = Flask(__name__)
uri = "bolt://localhost:7687"
user = "neo4j"
password = "root@420"
driver = GraphDatabase.driver(uri, auth=(user, password))

@app.route('/nodes')
def get_nodes():
    with driver.session() as session:
        result = session.run("MATCH (n) RETURN n LIMIT 10")
        nodes = [dict(record["n"]) for record in result]
        return jsonify(nodes)

if __name__ == '__main__':
    app.run()

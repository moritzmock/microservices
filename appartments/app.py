import sqlite3
import uuid
from flask import request
from flask import Flask
from flask import Response
import logging
import pika
import json
import consul
import time
import os

app = Flask(__name__)


@app.route("/add")
def add():
    id = uuid.uuid4()
    name = request.args.get("name")
    size = request.args.get("size")

    if name == None:
        return Response('{"result": false, "error": 1, "description": "Cannot proceed because you did not provide a name for the appartment."}', status=400, mimetype="application/json")
    if size == None:
            return Response('{"result": false, "error": 3, "description": "Cannot proceed because you did not provide a size for the appartment."}', status=400, mimetype="application/json")

    # Connect and setup the database
    connection = sqlite3.connect("/home/data/appartments.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text, size text)")

    # Check if appartement already exists
    cursor.execute("SELECT COUNT(id) FROM appartments WHERE name = ?", (name,))
    already_exists = cursor.fetchone()[0]
    if already_exists > 0:
        return Response('{"result": false, "error": 2, "description": "Cannot proceed because this appartment already exists"}', status=400, mimetype="application/json")

    # Add appartement
    cursor.execute("INSERT INTO appartments VALUES (?, ?, ?)", (str(id), name, size))
    cursor.close()
    connection.close()

    # Notify everybody that the appartment was added
    connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
    channel = connection.channel()
    channel.exchange_declare(exchange="appartments", exchange_type="direct")
    channel.basic_publish(exchange="appartments", routing_key="added", body=json.dumps({"id": str(id), "name": name}))
    connection.close()

    return Response('{"result": true, description="Appartment was added successfully."}', status=201, mimetype="application/json")

@app.route("/remove")
def remove():

    name = request.args.get("name")

    if name == None:
        return Response('{"result": false, "error": 1, "description": "Cannot proceed because you did not provide a name for the appartment."}',status=400, mimetype="application/json")

    # Connect and setup the database
    connection = sqlite3.connect("/home/data/appartments.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text, size text)")

    # Check if appartement exists
    cursor.execute("SELECT COUNT(id) FROM appartments WHERE name = ?", (name,))
    exists = cursor.fetchone()[0]

    if exists == 0:
        return Response(
            '{"result": false, "error": 2, "description": "Can not proceed because this appartment does not exists"}',
            status=400, mimetype="application/json")

    # remove appartement
    cursor.execute("DELETE FROM appartments WHERE name = (?)", (name,))
    cursor.close()
    connection.close()

    # Notify everybody that the appartment was removed
    connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
    channel = connection.channel()
    channel.exchange_declare(exchange="appartments", exchange_type="direct")
    channel.basic_publish(exchange="appartments", routing_key="removed", body=json.dumps({"name": name}))
    connection.close()

    return Response('{"result": true, description="Appartment was removed successfully."}', status=201, mimetype="application/json")

@app.route("/")
def hello():
    # Connect and setup the database
    connection = sqlite3.connect("/home/data/appartments.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text, size text)")

    cursor.execute("SELECT COUNT(id) FROM appartments")
    exists = cursor.fetchone()[0]
    return str(exists)

@app.route("/appartments")
def appartments():
    if os.path.exists("/home/data/appartments.db"):
        connection = sqlite3.connect("/home/data/appartments.db", isolation_level=None)
        cursor = connection.cursor()
        cursor.execute("SELECT id, name, size FROM appartments")
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return json.dumps({"appartments": rows})

    return json.dumps({"appartments": []})

def register():
    time.sleep(10)
    while True:
        try:
            connection = consul.Consul(host='consul', port=8500)
            connection.agent.service.register("appartments", address="appartments", port=5000)
            break
        except (ConnectionError, consul.ConsulException): 
            logging.warning('Consul is down, reconnecting...') 
            time.sleep(5) 

if __name__ == "__main__":
    logging.info("Starting the web server.")

    register()

    app.run(host="0.0.0.0", threaded=True)
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
    start = request.args.get("start")
    duration = request.args.get("duration")
    vip = request.args.get("vip")

    if name == None:
        return Response('{"result": false, "error": 1, "description": "Cannot proceed because you did not provide a name for the appartment."}', status=400, mimetype="application/json")

    if start == None:
            return Response('{"result": false, "error": 3, "description": "Cannot proceed because you did not provide a start for the appartment."}', status=400, mimetype="application/json")

    if duration == None:
        duration = "1"

    if vip == None:
        vip = "0"

    # Connect and setup the database
    connection = sqlite3.connect("/home/data/reserve.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS reserve (id text, name text, start text, duration text, vip text)")

    # Check if reservation already exists
    cursor.execute("SELECT COUNT(id) FROM reserve WHERE name = ? AND start = ?", (name,start,))
    already_exists = cursor.fetchone()[0]
    if already_exists > 0:
        return Response('{"result": false, "error": 2, "description": "Cannot proceed because this appartment already reserved"}', status=400, mimetype="application/json")

    # Add appartement
    cursor.execute("INSERT INTO reserve VALUES (?, ?, ?, ?, ?)", (str(id), name, start, duration, vip))
    cursor.close()
    connection.close()

    # Notify everybody that the appartment was added
    connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
    channel = connection.channel()
    channel.exchange_declare(exchange="reserve", exchange_type="direct")
    channel.basic_publish(exchange="reserve", routing_key="added", body=json.dumps({"id": str(id), "name": name, "start": start, "duration": duration}))
    connection.close()

    return Response('{"result": true, description="Appartment was booked successfully."}', status=201, mimetype="application/json")

@app.route("/remove")
def remove():

    id = request.args.get("id")

    if id == None:
        return Response('{"result": false, "error": 1, "description": "Cannot proceed because you did not provide an id for the appartment."}',status=400, mimetype="application/json")

    # Connect and setup the database
    connection = sqlite3.connect("/home/data/reserve.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS reserve (id text, name text, start text, duration text, vip text)")

    # Check if appartement exists
    cursor.execute("SELECT COUNT(id) FROM reserve WHERE id = ?", (id,))
    exists = cursor.fetchone()[0]

    if exists == 0:
        return Response(
            '{"result": false, "error": 2, "description": "Can not proceed because this reservation was not found"}',
            status=400, mimetype="application/json")

    # remove reservation
    cursor.execute("DELETE FROM reserve WHERE id = (?)", (id,))
    cursor.close()
    connection.close()

    # Notify everybody that the reservation was removed
    connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
    channel = connection.channel()
    channel.exchange_declare(exchange="appartments", exchange_type="direct")
    channel.basic_publish(exchange="appartments", routing_key="removed", body=json.dumps({"id": id}))
    connection.close()

    return Response('{"result": true, description="Reservation was removed successfully."}', status=201, mimetype="application/json")

@app.route("/")
def hello():
    # Connect and setup the database
    connection = sqlite3.connect("/home/data/reserve.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS reserve (id text, name text, start text, duration text, vip text)")

    cursor.execute("SELECT COUNT(id) FROM reserve")
    exists = cursor.fetchone()[0]
    return str(exists)

@app.route("/reservations")
def appartments():
    if os.path.exists("/home/data/reserve.db"):
        connection = sqlite3.connect("/home/data/reserve.db", isolation_level=None)
        cursor = connection.cursor()
        cursor.execute("SELECT id, name, start, duration, vip FROM reserve")
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return json.dumps({"reservations": rows})

    return json.dumps({"reservations": []})

def register(): 
    while True:
        try:
            connection = consul.Consul(host='consul', port=8500)
            connection.agent.service.register("reserve", address="reserve", port=5000)
            break
        except (ConnectionError, consul.ConsulException): 
            logging.warning('Consul is down, reconnecting...') 
            time.sleep(5) 

if __name__ == "__main__":
    logging.info("Starting the web server.")

    register()

    app.run(host="0.0.0.0", threaded=True)
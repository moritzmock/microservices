import sqlite3
import logging
import pika
import time
import threading
import json
from flask import Flask
from flask import request
from flask import Response
import consul
import os
import requests
import time
import uuid
from datetime import datetime, timedelta


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
            return Response('{"result": false, "error": 2, "description": "Cannot proceed because you did not provide a start for the appartment."}', status=400, mimetype="application/json")

    if duration == None:
        duration = "1"

    if vip == None:
        vip = "0"

    # Connect and setup the database
    connection = sqlite3.connect("/home/data/reserve.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS reserve (id text, name text, start text, duration text, vip text)")
    cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text)")

    # Check if apartment exists
    cursor.execute("SELECT COUNT(id) FROM appartments WHERE name = ?", (name,))
    exists = cursor.fetchall()[0]

    if exists[0] == 0:
        return Response('{"result": false, "error": 3, "description": "Can not proceed because this appartment is not valid"}',status=400, mimetype="application/json")

    cursor.execute("SELECT COUNT(id) FROM reserve")

    numberOfBookings = cursor.fetchone()[0]


    if numberOfBookings != 0:
        # Check if reservation already exists

        cursor.execute("SELECT COUNT(id) FROM reserve")

        numberOfBookings = cursor.fetchone()[0]

        cursor.execute("SELECT start, duration FROM reserve WHERE name = ?", (name,))
        for row in cursor.fetchall():
            dateStr = row[0]
            durationStr = row[1]
            dateExistingStart = datetime.strptime(str(dateStr), '%Y%m%d')
            dateWantedStart = datetime.strptime(str(start), '%Y%m%d')

            for i in range(int(durationStr) + 1):
                year = dateExistingStart.year
                month = dateExistingStart.month
                day = dateExistingStart.day

                if month < 10:
                    month = '0' + str(month)

                if day < 10:
                    day = '0' + str(day)

                compare = str(year) + str(month) + str(day)
                if str(start) == str(compare):
                    return Response('{"result": false, "error": 4, "description": "Can not proceed because this appartment is already reserved"}', status=400, mimetype="application/json")
                dateExistingStart = dateExistingStart + timedelta(days=1)

            for i in range(int(duration) + 1):
                year = dateWantedStart.year
                month = dateWantedStart.month
                day = dateWantedStart.day

                if month < 10:
                    month = '0' + str(month)

                if day < 10:
                    day = '0' + str(day)

                compare = str(year) + str(month) + str(day)
                if str(dateStr) == str(compare):
                    return Response('{"result": false, "error": 4, "description": "Can not proceed because this appartment is already reserved"}', status=400, mimetype="application/json")
                dateWantedStart = dateWantedStart + timedelta(days=1)


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


@app.route("/")
def hello():
    # Connect and setup the database
    connection = sqlite3.connect("/home/data/reserve.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text)")

    cursor.execute("SELECT COUNT(id) FROM appartments")
    numberApparments = cursor.fetchone()[0]


    cursor.execute("CREATE TABLE IF NOT EXISTS reserve (id text, name text, start text, duration text, vip text)")

    cursor.execute("SELECT COUNT(id) FROM reserve")
    numberReservation= cursor.fetchone()[0]
    return "- Number of apartments: " + str(numberApparments) + "<br/>- Number of reservation: " + str(numberReservation)


@app.route("/reservations")
def reserve():
    if os.path.exists("/home/data/reserve.db"):
        connection = sqlite3.connect("/home/data/reserve.db", isolation_level=None)
        cursor = connection.cursor()
        cursor.execute("SELECT id, name, start, duration, vip FROM reserve")
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return json.dumps({"reservationions": rows})

    return json.dumps({"reservationions": []})


@app.route("/remove")
def delete():
    id = request.args.get("id")

    if id == None:
        return Response('{"result": false, "error": 1, "description": "Cannot proceed because you did not provide an id for the reservation to delete."}',status=400, mimetype="application/json")

    # Connect and setup the database
    connection = sqlite3.connect("/home/data/reserve.db", isolation_level=None)
    cursor = connection.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS reserve (id text, name text, start text, duration text, vip text)")

    # Check if reservation exists
    cursor.execute("SELECT COUNT(id) FROM reserve WHERE id = ?", (id,))
    exists = cursor.fetchone()[0]

    if exists == 0:
        return Response('{"result": false, "error": 2, "description": "Can not proceed because this reservation was not found"}',status=400, mimetype="application/json")

    # remove reservation
    cursor.execute("DELETE FROM reserve WHERE id = (?)", (id,))
    cursor.close()
    connection.close()

    # Notify everybody that the reservation was deleted
    connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
    channel = connection.channel()
    channel.exchange_declare(exchange="reserve", exchange_type="direct")
    channel.basic_publish(exchange="reserve", routing_key="deleted",
                          body=json.dumps({"id": str(id)}))
    connection.close()

    return Response('{"result": true, "description": "Reservation was deleted successfully."}', status=201,mimetype="application/json")

@app.route("/appartments")
def appartments():
    if os.path.exists("/home/data/reserve.db"):
        connection = sqlite3.connect("/home/data/reserve.db", isolation_level=None)
        cursor = connection.cursor()
        cursor.execute("SELECT id, name FROM appartments")
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return json.dumps({"reserve appartments": rows})

    return json.dumps({"reserve appartments": []})

def appartment_added(ch, method, properties, body):
    logging.info("Apartment added message received.")
    data = json.loads(body)
    id = data["id"]
    name = data["name"]

    logging.info(f"Adding appartment {name}...")

    connection = sqlite3.connect("/home/data/reserve.db", isolation_level=None)
    cursor = connection.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text)")
    cursor.execute("INSERT INTO appartments VALUES (?, ?)", (id, name))
    cursor.close()
    connection.close()


def appartment_remove(ch, method, properties, body):
    logging.info("Apartment deleted message received.")
    data = json.loads(body)
    name = data["name"]

    logging.info(f"Deleting appartment {name}...")

    connection = sqlite3.connect("/home/data/reserve.db", isolation_level=None)
    cursor = connection.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text)")
    cursor.execute("DELETE FROM appartments WHERE name = ?", (name,))
    cursor.close()
    connection.close()


def register():
    time.sleep(10)
    while True:
        try:
            connection = consul.Consul(host='consul', port=8500)
            connection.agent.service.register("reserve", address="127.0.0.1", port=5002)
            break
        except (ConnectionError, consul.ConsulException):
            logging.warning('Consul is down, reconnecting...')
            time.sleep(5)

def connect_to_mq():
    while True:
        time.sleep(10)

        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
        except Exception as e:
            logging.warning(f"Could not start listening to the message queue, retrying...")

def listen_to_events(channel):
    channel.start_consuming()

def find_service(name):
    connection = consul.Consul(host="consul", port=8500)
    _, services = connection.health.service(name, passing=True)
    for service_info in services:
        address = service_info["Service"]["Address"]
        port = service_info["Service"]["Port"]
        return address, port

    return None, None

def deregister():
    connection = consul.Consul(host='consul', port=8500)
    connection.agent.service.deregister("reserve", address="reserve", port=5003)



if __name__ == "__main__":
    logging.info("Starting the web server.")

    register()

    connection = connect_to_mq()

    channel = connection.channel()

    # appartments
    channel.exchange_declare(exchange="appartments", exchange_type="direct")

    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange="appartments", queue=queue_name, routing_key="added")
    channel.basic_consume(queue=queue_name, on_message_callback=appartment_added, auto_ack=True)
    logging.info("Waiting for messages.")

    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange="appartments", queue=queue_name, routing_key="removed")
    channel.basic_consume(queue=queue_name, on_message_callback=appartment_remove, auto_ack=True)
    logging.info("Waiting for messages.")

    thread = threading.Thread(target=listen_to_events, args=(channel,), daemon=True)
    thread.start()

    # Verify if database has to be initialized
    database_is_initialized = False
    if os.path.exists("/home/data/reserve.db"):
        database_is_initialized = True
    else:
        connection = sqlite3.connect("/home/data/reserve.db", isolation_level=None)
        cursor = connection.cursor()

        # appartments
        cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text)")
        address, port = find_service("appartments")

        if address is not None and port is not None:
            response = requests.get(f"http://{address}:{port}/appartments")
            data = response.json()

            logging.info("Data received: " + data)

            for entry in data["appartments"]:
                cursor.execute("INSERT INTO appartments VALUES (?, ?, ?)", (entry["id"], entry["name"]))

            database_is_initialized = True

    if not database_is_initialized:
        logging.error("Cannot initialize database.")
    else:
        logging.info("Starting the web server.")

        try:
            app.run(host="0.0.0.0", threaded=True)
        finally:
            connection.close()
            deregister()


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
from datetime import datetime, timedelta

app = Flask(__name__)


@app.route("/")
def hello():
    # Connect and setup the database
    connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text)")

    cursor.execute("SELECT COUNT(id) FROM appartments")
    numberApparments = cursor.fetchone()[0]

    cursor.execute("CREATE TABLE IF NOT EXISTS reserve (id text, name text, start text, duration text)")

    cursor.execute("SELECT COUNT(id) FROM reserve")
    numberReservation= cursor.fetchone()[0]
    return "- Number of apartments: " + str(numberApparments) + "<br/>- Number of reservation: " + str(numberReservation)


@app.route("/search")
def search():
    start = request.args.get("start")
    duration = request.args.get("duration")

    if start == None:
        return Response('{"result": false, "error": 1, "description": "Cannot proceed because you did not provide a start for the search."}',status=400, mimetype="application/json")

    if duration == None:
        return Response('{"result": false, "error": 1, "description": "Cannot proceed because you did not provide a duration date for the reservation."}',status=400, mimetype="application/json")

    duration = int(duration)

    # Connect and setup the database
    connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS reserve (id text, name text, start text, duration text)")
    cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text)")

    # Check for free apartments
    cursor.execute("SELECT name from appartments")
    data = cursor.fetchall()
    result = []
    if len(data) == 0:
        return Response('{"result": false, "error": 2, "description": "No apparmetns in the database"}',status=400, mimetype="application/json")

    for app in data:
        cursor.execute("SELECT name, start, duration from reserve WHERE name = ?", (app[0],))
        reservations = cursor.fetchall()
        if(len(reservations) == 0):
            result.append(app[0])
        else:
            checkIfFree = True

            for res in reservations:
                dateStr = res[1]
                durationStr = res[2]
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
                        checkIfFree = False
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
                        checkIfFree = False
                    dateWantedStart = dateWantedStart + timedelta(days=1)

            if checkIfFree == True:
                result.append(app[0])


    list += "Names of the available appartments"
    for record in result:
        list += f"<div>{record}</div>\n"

    connection.close()

    return f'<p>There are {len(result)} out of {len(data)} accommodations available for the desired holiday duration.</p><p>{list}</p>'


def appartment_added(ch, method, properties, body):
    logging.info("Apartment added message received.")
    data = json.loads(body)
    id = data["id"]
    name = data["name"]

    logging.info(f"Adding appartment {name}...")

    connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
    cursor = connection.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text)")
    cursor.execute("INSERT INTO appartments VALUES (?, ?)", (id, name))
    cursor.close()
    connection.close()


def appartment_removed(ch, method, properties, body):
    logging.info("Apartment deleted message received.")
    data = json.loads(body)
    name = data["name"]

    logging.info(f"Deleting appartment {name}...")

    connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
    cursor = connection.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text)")
    cursor.execute("DELETE FROM appartments WHERE name = ?", (name,))
    cursor.close()
    connection.close()


def reservation_added(ch, method, properties, body):
    logging.info("Reservation added message received.")
    data = json.loads(body)
    id = data["id"]
    name = data["name"]
    start = data["start"]
    duration = data["duration"]

    logging.info(f"Adding reservation {id}({name})...")

    connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
    cursor = connection.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS reserve (id text, name text, start text, duration text)")
    cursor.execute("INSERT INTO reserve VALUES (?, ?, ?, ?)", (id, name, start, duration))
    cursor.close()
    connection.close()


def reservation_removed(ch, method, properties, body):
    logging.info("Reservation deleted message received.")
    data = json.loads(body)
    id = data["id"]

    logging.info(f"Deleting reservation {id}...")

    connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
    cursor = connection.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS reserve (id text, name text, start text, duration text)")
    cursor.execute("DELETE FROM reserve WHERE id = ?", (id,))
    cursor.close()
    connection.close()


def connect_to_mq():
    while True:
        time.sleep(10)

        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
        except Exception as e:
            logging.warning(f"Could not start listening to the message queue, retrying...")


def listen_to_events(channel):
    channel.start_consuming()


def register():
    time.sleep(10)
    while True:
        try:
            connection = consul.Consul(host='consul', port=8500)
            connection.agent.service.register("search", address="127.0.0.1", port=5003)
            break
        except (ConnectionError, consul.ConsulException):
            logging.warning('Consul is down, reconnecting...')
            time.sleep(5)


def deregister():
    connection = consul.Consul(host='consul', port=8500)
    connection.agent.service.deregister("search", address="search", port=5002)


def find_service(name):
    connection = consul.Consul(host="consul", port=8500)
    _, services = connection.health.service(name, passing=True)
    for service_info in services:
        address = service_info["Service"]["Address"]
        port = service_info["Service"]["Port"]
        return address, port

    return None, None


if __name__ == "__main__":
    logging.basicConfig(format="%(message)s", level=1 * 10)
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.getLogger("sqlite3").setLevel(logging.WARNING)

    logging.info("Start.")

    register()

    connection = connect_to_mq()

    channel = connection.channel()
    channel.exchange_declare(exchange="appartments", exchange_type="direct")

    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange="appartments", queue=queue_name, routing_key="added")
    channel.basic_consume(queue=queue_name, on_message_callback=appartment_added, auto_ack=True)

    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange="appartments", queue=queue_name, routing_key="removed")
    channel.basic_consume(queue=queue_name, on_message_callback=appartment_removed, auto_ack=True)

    channel.exchange_declare(exchange="reserve", exchange_type="direct")

    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange="reserve", queue=queue_name, routing_key="added")
    channel.basic_consume(queue=queue_name, on_message_callback=reservation_added, auto_ack=True)

    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange="reserve", queue=queue_name, routing_key="removed")
    channel.basic_consume(queue=queue_name, on_message_callback=reservation_removed, auto_ack=True)

    logging.info("Waiting for messages.")

    thread = threading.Thread(target=listen_to_events, args=(channel,), daemon=True)
    thread.start()

    # Verify if database has to be initialized
    database_is_initialized = False
    if os.path.exists("/home/data/search.db"):
        database_is_initialized = True
    else:
        connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text)")

        address, port = find_service("appartments")
        if address is not None and port is not None:
            response = requests.get(f"http://{address}:{port}/appartments")
            data = response.json()

            logging.info("Received data: " + data)

            for entry in data["appartments"]:
                cursor.execute("INSERT INTO appartments VALUES (?, ?, ?)", (entry["id"], entry["name"]))

            database_is_initialized = True

        cursor.execute(
            "CREATE TABLE IF NOT EXISTS reserve (id text, name text, start text, duration text)")

        address, port = find_service("reserve")
        if address is not None and port is not None:
            response = requests.get(f"http://{address}:{port}/reservations")
            data = response.json()

            logging.info("Received data: " + data)

            for entry in data["reservation"]:
                cursor.execute("INSERT INTO reservation VALUES (?, ?, ?, ?, ?)",
                               (entry["id"], entry["name"], entry["start"], entry["duration"]))

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

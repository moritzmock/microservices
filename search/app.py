import sqlite3
import logging
import pika
import time
import threading
import json
from flask import Flask
import consul
import os
import requests

app = Flask(__name__)


@app.route("/")
def hello():
    # Connect and setup the database
    connection = sqlite3.connect("/home/data/appartments.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text, size text)")

    cursor.execute("SELECT COUNT(id) FROM appartments")
    numberApparments = cursor.fetchone()[0]

    connection = sqlite3.connect("/home/data/reserve.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS reserve (id text, name text, size text)")

    cursor.execute("SELECT COUNT(id) FROM reserve")
    numberReservation= cursor.fetchone()[0]
    return "- Number of apartments: " + str(numberApparments) + "<br/>- Number of reservation: " + str(numberReservation)


def appartment_added(ch, method, properties, body):
    data = json.loads(body)
    id = data["id"]
    name = data["name"]

    logging.info(f"Adding appartment {name}...")

    connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("INSERT INTO appartments VALUES (?, ?)", (id, name))
    cursor.close()
    connection.close()

def appartment_removed(ch, method, properties, body):
    data = json.loads(body)
    name = data["name"]

    logging.info(f"Removing appartment {name}...")

    connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
    cursor = connection.cursor()

    cursor.execute("SELECT COUNT(id) FROM appartments WHERE name = ?", (name,))
    exists = cursor.fetchone()[0]

    #only delete if it is in the DB
    if exists > 0:
        cursor.execute("DELETE FROM appartments WHERE name = (?)", (name,))

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
            connection.agent.service.register("search", address="search", port=5000)
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

    channel.queue_bind(exchange="appartments", queue=queue_name, routing_key="removed")
    channel.basic_consume(queue=queue_name, on_message_callback=appartment_removed, auto_ack=True)

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
        if address != None and port != None:
            response = requests.get(f"http://{address}:{port}/appartments")
            data = response.json()

            for entry in data["appartments"]:
                cursor.execute("INSERT INTO appartments VALUES (?, ?)", (entry["id"], entry["name"]))

            database_is_initialized = True

        # Setup Database for reserve and get the entries from rabbitMQ
        cursor.execute("CREATE TABLE IF NOT EXISTS reserve (id text, name text, start text, duration text, vip text)")

        address, port = find_service("reserve")
        if address != None and port != None:
            response = requests.get(f"http://{address}:{port}/reserve")
            data = response.json()

            for entry in data["reserve"]:
                cursor.execute("INSERT INTO reserve VALUES (?, ?, ?, ?)", (entry["id"], entry["name"], entry["start"], entry["duration"]))

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
from flask import Flask, escape, request
from threading import Event
import signal

from flask_kafka import FlaskKafka

app = Flask(__name__)

@app.route('/')
def hello():
    name = request.args.get("name", "World")
    return f'Hello, {escape(name)}!'

INTERRUPT_EVENT = Event()

bus = FlaskKafka(INTERRUPT_EVENT,
    bootstrap_servers=",".join(["127.0.0.1:9092"]),
    group_id="consumer-grp-id"
)
KAFKA_TOPIC = 'test'

def listen_kill_server():
    signal.signal(signal.SIGTERM, bus.interrupted_process)
    signal.signal(signal.SIGINT, bus.interrupted_process)
    signal.signal(signal.SIGQUIT, bus.interrupted_process)
    signal.signal(signal.SIGHUP, bus.interrupted_process)


@bus.handle(KAFKA_TOPIC)
def test_topic_handler(msg):
    print("consumed {} from test topic".format(msg))


if __name__ == '__main__':
    bus.run()
    listen_kill_server()
    app.run(debug=True, port=5004)
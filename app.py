from flask import Flask, request
from json import dumps
from cassandra_client import CassandraClient
import sys

app = Flask(__name__)


@app.route("/", methods=['POST'])
def main():
    data = request.get_json()

    method = None
    try:
        method = getattr(client, f'select_{data["type"]}')
    except AttributeError:
        raise NotImplementedError(data["type"])

    return dumps(method(data), indent=4, default=str)


if __name__ == "__main__":
    client = CassandraClient()
    client.connect()

    app.run(host='0.0.0.0', debug=True)

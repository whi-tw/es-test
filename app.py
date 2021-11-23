import os
import json
from typing import Union
import elasticsearch
from flask import Flask, url_for, jsonify, request
from datetime import datetime

import opensearchpy

VCAP_SERVICES: dict = json.loads(os.getenv("VCAP_SERVICES"))
TYPE = [
    kind for kind in VCAP_SERVICES.keys() if kind in ["elasticsearch", "opensearch"]
][0]
if TYPE == "opensearch":
    from opensearchpy import OpenSearch as SearchClient
elif TYPE == "elasticsearch":
    from elasticsearch import Elasticsearch as SearchClient
else:
    print("not bound to elasticsearch or opensearch")
    exit(1)
CREDENTIALS = VCAP_SERVICES[TYPE][0]["credentials"]
HOST = CREDENTIALS["uri"]
INDEX = "tweets"

print("Elastic host is {}".format(CREDENTIALS["hostname"]))

client: Union[opensearchpy.OpenSearch, elasticsearch.Elasticsearch] = SearchClient(
    [HOST]
)
app = Flask(__name__)


def create_tweet(
    id: int,
    author: str = "kimchy",
    text: str = "Elasticsearch: cool. bonsai cool.",
    timestamp: datetime = datetime.now(),
):
    doc = {
        "author": author,
        "text": text,
        "timestamp": timestamp,
    }
    res = client.create(INDEX, id, doc_type="tweet", body=doc)
    client.indices.refresh(index=INDEX)
    return "created"


def jsonify_error(error: Exception):
    out = {"error": {"type": type(error).__name__, "message": str(error)}}
    return out


@app.route("/info")
def api_info():
    return jsonify(client.info())


@app.route("/health")
def api_health():
    return jsonify(client.cluster.health())


@app.route("/")
def api_root():
    return f"Welcome to the flask API with {TYPE} backend. Try /health endpoint"


@app.route("/index/<id>", methods=["GET", "PUT", "DELETE"])
def index_doc(id):
    if request.method == "PUT":
        try:
            return create_tweet(id)
        except Exception as e:
            return jsonify_error(e)
    if request.method == "DELETE":
        try:
            res = client.delete(index=INDEX, id=id)
            return res
        except Exception as e:
            return jsonify_error(e)
    else:
        try:
            res = client.get(index=INDEX, id=id)
            return res
        except Exception as e:
            return jsonify_error(e)


if __name__ == "__main__":
    try:
        create_tweet(-1) # force creation of index if it does not exist
    except:
        pass
    app.run(debug=True, host="0.0.0.0", port=os.getenv("PORT", 8080))

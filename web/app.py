import flask
import json
import random

from flask import request
from id_generator.idgenerator import IDGenerator
from id_generator.shard import get_shards

app = flask.Flask("App")


@app.route("/id")
def get_id():
    _id = IDGenerator(1).get_id()
    return json.dumps({"id": _id, "id_str": str(_id)})


@app.route("/ids")
def get_ids():
    count = request.args.get("count") or 10
    ids = list(IDGenerator(1).get_ids(int(count)))
    return json.dumps({"ids": ids, "ids_str": [str(i) for i in ids]})


@app.route("/shard/id")
def get_id_from_shard():
    shards = get_shards()
    shard_id = request.args.get("shard_id") or random.randrange(1, len(shards))
    _id = IDGenerator(int(shard_id)).get_id()
    return json.dumps({"id": _id, "id_str": str(_id)})


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)

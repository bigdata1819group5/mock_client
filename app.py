from uuid import uuid4
import time
import json

import gpxpy
import requests

TOPIC = 'vehiclelocation'
HOST = 'http://localhost:8000'

FN = 'data/tehran_0.gpx'
URL = '{}/topics/{}'.format(HOST, TOPIC)


def produce(filename):
    gpx_file = open(filename, 'r')

    gpx = gpxpy.parse(gpx_file)

    for track in gpx.tracks:
        for segment in track.segments:
            last = None
            cid = str(uuid4())
            for point in segment.points:
                if last and (point.time - last).seconds:
                    time.sleep((point.time - last).seconds)
                last = point.time
                send(point, cid)


def send(point, cid):
    data = {
        'records': [{'value': '{},{},{},{}'.format(
            cid, str(point.time.replace(tzinfo=None)), point.latitude, point.longitude,
        )}]
    }

    headers = {
        'Accept': "application/vnd.kafka.v2+json",
        'Content-Type': "application/vnd.kafka.json.v2+json",
    }

    resp = requests.request("POST", URL, data=json.dumps(data), headers=headers)
    print(resp)


if __name__ == "__main__":
    produce(FN)

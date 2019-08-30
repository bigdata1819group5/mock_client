from datetime import datetime
import random
from uuid import uuid4
from os import listdir
from os.path import join
import time
import json

import gpxpy
from locust import HttpLocust, TaskSet, task


TOPIC = 'vehiclelocation'
DATA_DIR = 'data/'

TRACES_SET = list()
CO_ID_LIST = [1, 2]


class Trace(TaskSet):

    def on_start(self):
        self.vehicle_id = str(uuid4())
        self.co_id = random.choice(CO_ID_LIST)
        self.trace = random.choice(TRACES_SET)

    @task
    def submit_trace(self):
        last = None
        for point in self.trace.points:
            if last and (point.time - last).seconds:
                time.sleep((point.time - last).seconds)
            last = point.time
            self.send(point, self.vehicle_id, self.co_id)

    def send(self, point, vehicle_id, company_id):
        point.time = point.time or datetime.utcnow()
        data = {
            'records': [{'value': '{},{},{},{},{}'.format(
                vehicle_id, company_id, str(point.time.replace(tzinfo=None)),
                point.latitude, point.longitude,
            )}]
        }

        headers = {
            'Accept': "application/vnd.kafka.v2+json",
            'Content-Type': "application/vnd.kafka.json.v2+json",
        }
        path = '/topics/{}'.format(TOPIC)
        self.client.post(path, data=json.dumps(data), headers=headers)


class Vehicle(HttpLocust):
    host = 'http://localhost:8000'
    task_set = Trace
    min_wait = 100
    max_wait = 200

    def setup(self):
        # load traces data
        onlyfiles = [join(DATA_DIR, f) for f in listdir(DATA_DIR)]

        for gpx_file in onlyfiles:
            gpx = gpxpy.parse(open(gpx_file, 'r'))

            for track in gpx.tracks:
                TRACES_SET.extend(track.segments)

### Sample Client

Set server address and topic name in `app.py` and run:

```
python app.py
```

### Load Test

Load test has been implemented using `Locustio`. It creates many mock users,
which choose a random track segment from dataset and pick a randam vehicle ID and
company ID and start submitting location data using the track segment.

To run:
- -c: number of clients to mock
- -r: number of clients to spawn per second

```
locust -f load.py --no-web -c 1000 -r 100
```
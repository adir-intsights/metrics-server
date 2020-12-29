# Metrics Server

Collects and analyzes GKE resource metrics like CPU and memory utilization.

## Usage
Install the package:
```
$ python setup.py install
```

Start the server:
```
$ PROJECT=intsights uvicorn metrics_server.metrics_server:app
```

Request memory metrics:
```
$ curl localhost:8000/metrics/memory
```

Request CPU metrics:
```
$ curl localhost:8000/metrics/cpu
```

By default the last 14 days are being queried, you can change that with the
`duration_days` parameter. The default output format is a human readable table,
you can change it to JSON with the `output_format` parameter, e.g:
```
$ curl localhost:8000/metrics/cpu?duration_days=30&output_format=json
```

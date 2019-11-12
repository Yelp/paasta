from statistics import mean


def uwsgi_parser(json):
    workers = json["workers"]
    utilization = [1.0 if worker["status"] != "idle" else 0.0 for worker in workers]
    return mean(utilization) * 100


def http_parser(json):
    return float(json["utilization"]) * 100

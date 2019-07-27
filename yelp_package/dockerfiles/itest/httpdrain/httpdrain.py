import datetime
import errno
import os
from wsgiref.simple_server import make_server

from pyramid.config import Configurator
from pyramid.response import Response

DRAIN_FILE = "drain"


def drain(request):
    if not os.path.exists(DRAIN_FILE):
        with open(DRAIN_FILE, "w+") as f:
            f.write(str(datetime.datetime.now().timestamp()))
    return Response(status_int=200)


def stop_drain(request):
    try:
        os.remove(DRAIN_FILE)
        return Response(status_int=200)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
        else:
            return Response(status_int=200)


def status_drain(request):
    if os.path.exists(DRAIN_FILE):
        return Response(status_int=200)
    else:
        return Response(status_int=400)


def safe_to_kill(request):
    if os.path.exists(DRAIN_FILE):
        with open(DRAIN_FILE) as f:
            dt = datetime.datetime.fromtimestamp(float(f.read()))
            delta = datetime.datetime.now() - dt
            if delta.seconds > 2:
                return Response(status_int=200)
            else:
                return Response(status_int=400)
    else:
        return Response(status_int=400)


if __name__ == "__main__":
    with Configurator() as config:
        config.add_route("drain", "/drain")
        config.add_route("stop_drain", "/drain/stop")
        config.add_route("drain_status", "/drain/status")
        config.add_route("drain_safe_to_kill", "/drain/safe_to_kill")
        config.add_view(drain, route_name="drain")
        config.add_view(stop_drain, route_name="stop_drain")
        config.add_view(status_drain, route_name="drain_status")
        config.add_view(safe_to_kill, route_name="drain_safe_to_kill")
        app = config.make_wsgi_app()
    server = make_server("0.0.0.0", 3000, app)
    server.serve_forever()

FROM docker-dev.yelpcorp.com/xenial_yelp

COPY ./uwsgi_exporter/uwsgi_exporter /bin/uwsgi_exporter
ENV STATS_PORT=8889
CMD /bin/uwsgi_exporter --stats.uri "http://127.0.0.1:${STATS_PORT}/" --web.listen-address :9117

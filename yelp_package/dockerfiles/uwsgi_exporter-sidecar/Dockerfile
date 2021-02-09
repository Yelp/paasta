FROM docker-dev.yelpcorp.com/xenial_yelp

COPY ./uwsgi_exporter/uwsgi_exporter /bin/uwsgi_exporter
ENV STATS_URI=http://127.0.0.1:8889/
CMD /bin/uwsgi_exporter --stats.uri "${STATS_URI}" --web.listen-address :9117

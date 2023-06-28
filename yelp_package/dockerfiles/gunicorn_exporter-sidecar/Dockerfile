FROM docker-dev.yelpcorp.com/jammy_yelp

COPY ./statsd_exporter/statsd_exporter /bin/statsd_exporter
RUN mkdir /etc/statsd_exporter
COPY gunicorn.mapping /etc/statsd_exporter/gunicorn.mapping

ENV STATS_INGESTION_PORT=8889
ENV HTTP_LISTEN_PORT=9117

CMD /bin/statsd_exporter --statsd.listen-udp=":${STATS_INGESTION_PORT}" --web.listen-address=":${HTTP_LISTEN_PORT}" --statsd.listen-tcp="" --statsd.mapping-config=/etc/statsd_exporter/gunicorn.mapping --log.level=debug

FROM docker-dev.yelpcorp.com/xenial_yelp

COPY ./uwsgi_exporter/uwsgi_exporter /bin/uwsgi_exporter
CMD /bin/uwsgi_exporter --stats.uri http://127.0.0.1:8889/ --collect.cores false --web.listen-address :9117

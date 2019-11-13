FROM alpine:3.6

RUN apk add --no-cache --update \
    gcc \
    musl-dev \
    python3-dev \
    libffi-dev \
    openssl-dev \
    python3

ADD . /moto/
ENV PYTHONUNBUFFERED 1

WORKDIR /moto/
RUN  python3 -m ensurepip && \
     rm -r /usr/lib/python*/ensurepip && \
     pip3 install --upgrade pip setuptools && \
     # moto and botocore have mismatched upper-bound pins for python-dateutils
     # which breaks our build.  botocore used to have <3.0.0, but shrunk that to
     # <2.8.1, and moto hasn't updated their pin to match yet.  So until those
     # are fixed, here's the latest version of boto that has the <3.0.0 pin.
     #
     # We can unpin boto3 and botocore once botocore fixes its pin
     # (see https://github.com/boto/botocore/commit/e87e7a745fd972815b235a9ee685232745aa94f9)
     pip3 install botocore==1.13.8 boto3==1.10.8 "moto[server]"

ENTRYPOINT ["/usr/bin/moto_server", "-H", "0.0.0.0"]

EXPOSE 5000

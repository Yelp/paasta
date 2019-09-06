FROM ubuntu:bionic
RUN sed -i 's/archive.ubuntu.com/us-east1.gce.archive.ubuntu.com/g' /etc/apt/sources.list

RUN apt-get update > /dev/null && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        python3 \
        python3-pip
RUN pip3 install pyramid
ADD httpdrain.py /httpdrain.py
CMD ["python3", "/httpdrain.py"]

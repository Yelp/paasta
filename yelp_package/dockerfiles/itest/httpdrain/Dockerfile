FROM ubuntu:bionic

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        python3 \
        python3-pip
RUN pip3 install pyramid
ADD httpdrain.py /httpdrain.py
CMD ["python3", "/httpdrain.py"]

FROM    ubuntu:xenial
RUN     apt-get -yq update && apt-get install -yq --no-install-recommends software-properties-common
RUN     add-apt-repository ppa:deadsnakes/ppa
RUN     apt-get -yq update && apt-get install -yq --no-install-recommends \
            debhelper \
            dpkg-dev \
            gcc \
            gdebi-core \
            libfreetype6 \
            libatlas-base-dev \
            libyaml-dev \
            python3.7-dev \
            python-pip \
            wget
RUN     cd /tmp && \
        wget http://mirrors.kernel.org/ubuntu/pool/universe/d/dh-virtualenv/dh-virtualenv_1.0-1_all.deb && \
        gdebi -n dh-virtualenv*.deb && \
        rm dh-virtualenv_*.deb
WORKDIR /work

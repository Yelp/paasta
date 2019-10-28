#!/bin/bash

COMPOSE_CONTAINERS="zookeeper mesosmaster mesosagent moto-ec2 moto-s3 moto-dynamodb"

cleanup() {
    docker kill "${CONTAINER}" > /dev/null
    for compose_container in ${COMPOSE_CONTAINERS}; do
        docker network disconnect "clusterman_${DISTRIB_CODENAME}_acceptance" "clusterman_${DISTRIB_CODENAME}_${compose_container}_1"
    done
    docker network rm "clusterman_${DISTRIB_CODENAME}_acceptance" > /dev/null
}

setup_networks() {
    if [ "${DISTRIB_CODENAME}" == "xenial" ]; then
        CIDR_BLOCK="10.0.0.0/24"
    else
        CIDR_BLOCK="11.0.0.0/24"
    fi
    docker network create --ip-range "${CIDR_BLOCK}" --subnet "${CIDR_BLOCK}" "clusterman_${DISTRIB_CODENAME}_acceptance"
    for compose_container in ${COMPOSE_CONTAINERS}; do
        docker network connect "clusterman_${DISTRIB_CODENAME}_acceptance" "clusterman_${DISTRIB_CODENAME}_${compose_container}_1"
    done
}

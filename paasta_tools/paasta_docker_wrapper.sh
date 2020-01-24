#!/bin/bash
# This is just a performance optimisation. Python is slow
# especially when importing large projects like paasta_tools
# our wrapper isn't needed for docker inspect so lets fall
# back to regular docker fast if we are inspecting
USE_SYSTEM_DOCKER=0
for arg in "$@"; do
    if [ "$arg" = "inspect" ]; then
        USE_SYSTEM_DOCKER=1
    fi
done

if [ $USE_SYSTEM_DOCKER -eq 1 ]; then
    exec docker "$@"
else
    exec /usr/bin/paasta_docker_wrapper_python "$@"
fi

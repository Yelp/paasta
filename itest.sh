#!/bin/bash
source ./.paasta/bin/activate
tox -e general_itests
tox -e paasta_itests

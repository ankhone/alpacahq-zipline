#!/bin/bash

set -x

mkdir -p /root/zipline-algos/

# zipline run \
PYTHONPATH=/zipline \
python -mpdb ./zipline/__main__.py run \
    -f ./poc/momo1.py \
    --broker=alpaca --broker-uri=foo \
    --state-file ~/zipline-algos/demo.state \
    --realtime-bar-target ~/zipline-algos/realtime-bars/ \
    --bundle alpaca-bundle --data-frequency minute